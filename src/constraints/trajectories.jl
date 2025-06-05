export add_start_up_trajectory_lower_bound_constarints!
export add_start_up_trajectory_upper_bound_constraints!

"""
    add_start_up_trajectory_lower_bound_constraints!(model, constraints)

Adds the start up trajectory lower bound constraints to the model.
Assets using this constraint should have a minimum down time >= length of start up trajectory + length of shut down trajectory
"""
function add_trajectory_constraints!(
    connection,
    model,
    variables,
    expressions,
    constraints,
    profiles,
)
    let table_name = :trajectory, cons = constraints[table_name]
        # Prevent error if column doesnt exist
        if length(collect(cons.indices)) == 0
            return
        end

        indices = _append_data_to_trajectory(connection, table_name)

        # Expression for Profile * Capacity -> pmin
        attach_expression!(
            cons,
            :min_production,
            [
                @expression(
                    model,
                    row.min_operating_point *
                    row.capacity *
                    _profile_aggregate(
                        profiles.rep_period,
                        (row.profile_name, row.year, row.rep_period),
                        row.time_block_start:row.time_block_end,
                        Statistics.mean,
                        1.0,
                    ) *
                    cons.expressions[:units_on][row.id]
                ) for row in indices
            ],
        )

        # Expression for Profile * Capacity * units_on -> pmax
        attach_expression!(
            cons,
            :max_production,
            [
                @expression(
                    model,
                    row.capacity *
                    _profile_aggregate(
                        profiles.rep_period,
                        (row.profile_name, row.year, row.rep_period),
                        row.time_block_start:row.time_block_end,
                        Statistics.mean,
                        1.0,
                    ) *
                    cons.expressions[:units_on][row.id]
                ) for row in indices
            ],
        )

        flow_total = cons.expressions[:outgoing]
        pmin_sum = cons.expressions[:min_production]
        pmax_sum = cons.expressions[:max_production]
        start_up_zeroes = cons.expressions[:start_up]
        start_up = []
        shut_down_zeroes = cons.expressions[:shut_down]
        shut_down = []

        # Replace every zero in the start_up with the first non-zero after it
        for (i, su) in enumerate(start_up_zeroes)
            if (su == 0)
                while (su == 0 && i != length(start_up_zeroes))
                    i += 1
                    su = start_up_zeroes[i]
                end
            end
            push!(start_up, su)
        end

        # Shift to align with constraint
        for i in 1:length(start_up)-1
            start_up[i] = start_up[i+1]
        end
        start_up[length(start_up)] = 0

        # Replace every zero in shut_down with the first non-zero before it
        for (i, sd) in enumerate(shut_down_zeroes)
            if (sd == 0)
                while (sd == 0 && i >= 1)
                    i -= 1
                    sd = shut_down_zeroes[i]
                end
            end
            push!(shut_down, sd)
        end

        # Attach lower bound constraint
        attach_constraint!(
            model,
            cons,
            "start_up_$(table_name)_lower_bound" |> Symbol,
            [
                let su_traj = read_trajectory(row.start_trajectory),
                    t_su = length(su_traj),
                    sd_traj = read_trajectory(row.shut_trajectory),
                    t_sd = length(sd_traj),
                    t_start = row.time_block_start,
                    t_end = row.time_block_end

                    start_up_sum = 0
                    if (!ismissing(row.next_SU_start))
                        start_up_sum = sum(
                            collect([
                                if (
                                    t_start + i <= row.next_SU_start &&
                                    row.next_SU_start <= t_end + i
                                )
                                    su_traj[t_su-i+1]
                                else
                                    0
                                end for i in collect(1:t_su)
                            ]),
                        )
                    end

                    shut_down_sum = 0
                    if (!ismissing(row.last_SD_start))
                        shut_down_sum = sum(
                            collect([
                                if (
                                    t_start - i <= row.last_SD_start &&
                                    row.last_SD_start <= t_end - i
                                )
                                    sd_traj[i+1]
                                else
                                    0
                                end for i in collect(0:t_su-1)
                            ]),
                        )
                    end

                    @constraint(
                        model,
                        flow_total[row.id] >=
                        pmin_sum[row.id] +
                        (start_up[row.id] * start_up_sum + shut_down[row.id] * shut_down_sum) /
                        (row.time_block_end - row.time_block_start + 1),
                        base_name = "$table_name[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]"
                    )
                end for row in indices
            ],
        )

        # Attach upper bound constraint
        attach_constraint!(
            model,
            cons,
            "start_up_$(table_name)_upper_bound" |> Symbol,
            [
                let su_traj = read_trajectory(row.start_trajectory),
                    t_su = length(su_traj),
                    sd_traj = read_trajectory(row.shut_trajectory),
                    t_sd = length(sd_traj),
                    t_start = row.time_block_start,
                    t_end = row.time_block_end

                    start_up_sum = 0
                    if (!ismissing(row.next_SU_start))
                        start_up_sum = sum(
                            collect([
                                if (
                                    t_start + i <= row.next_SU_start &&
                                    row.next_SU_start <= t_end + i
                                )
                                    su_traj[t_su-i+1]
                                else
                                    0
                                end for i in collect(1:t_su)
                            ]),
                        )
                    end

                    shut_down_sum = 0
                    if (!ismissing(row.last_SD_start))
                        shut_down_sum = sum(
                            collect([
                                if (
                                    t_start - i <= row.last_SD_start &&
                                    row.last_SD_start <= t_end - i
                                )
                                    sd_traj[i+1]
                                else
                                    0
                                end for i in collect(0:t_su-1)
                            ]),
                        )
                    end

                    @constraint(
                        model,
                        flow_total[row.id] <=
                        pmax_sum[row.id] +
                        (start_up[row.id] * start_up_sum + shut_down[row.id] * shut_down_sum) /
                        (row.time_block_end - row.time_block_start + 1),
                        base_name = "$table_name[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]"
                    )
                end for row in indices
            ],
        )
    end
    return nothing
end

function _append_data_to_trajectory(connection, table_name)
    return DuckDB.query(
        connection,
        "SELECT
            cons.*,
            start_up.time_block_start AS next_SU_start,
            shut_down.time_block_start AS last_SD_start,
            asset.start_trajectory        AS start_trajectory,
            asset.shut_trajectory         AS shut_trajectory,
            asset.capacity          AS capacity,
            profiles.profile_name   AS profile_name,
            expr_avail.id           AS avail_id
        FROM cons_$table_name AS cons
        LEFT JOIN expr_available_asset_units_simple_method AS expr_avail
            ON cons.asset = expr_avail.asset
            AND cons.year = expr_avail.milestone_year
        LEFT JOIN asset AS asset
            ON cons.asset = asset.asset
        LEFT JOIN assets_profiles as profiles
            ON cons.asset = profiles.asset
            AND cons.year = profiles.commission_year
            AND profiles.profile_type = 'availability'
        LEFT JOIN asset_time_resolution_rep_period AS atr
            ON  cons.asset = atr.asset
            AND cons.year = atr.year
            AND cons.rep_period = atr.rep_period
            AND cons.time_block_start >= atr.time_block_start
            AND cons.time_block_end <= atr.time_block_end
        LEFT JOIN var_start_up AS start_up
            ON cons.asset = start_up.asset
            AND cons.year = start_up.year
            AND cons.rep_period = start_up.rep_period
            AND atr.time_block_end + 1 = start_up.time_block_start
        LEFT JOIN var_shut_down AS shut_down
            ON cons.asset = shut_down.asset
            AND cons.year = shut_down.year
            AND cons.rep_period = shut_down.rep_period
            AND atr.time_block_start = shut_down.time_block_start
        WHERE asset.investment_method in ('simple', 'none')
        ORDER BY cons.id
        ",
    )
end
