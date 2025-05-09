export add_start_up_trajectory_lower_bound_constarints!
export add_start_up_trajectory_upper_bound_constraints!

"""
    add_start_up_trajectory_lower_bound_constraints!(model, constraints)

Adds the start up trajectory lower bound constraints to the model.
"""
function add_start_up_trajectory_lower_bound_constraints!(
    connection,
    model,
    variables,
    expressions,
    constraints,
    profiles,
)
    let table_name = :start_up_trajectory_lower_bound, cons = constraints[table_name]
        indices = _append_data_to_start_up_trajectory(connection, table_name)

        # Expression for maximum available units per year
        expr_avail_simple_method =
            expressions[:available_asset_units_simple_method].expressions[:assets]

        # Expression for Profile * Capacity -> pmax
        attach_expression!(
            cons,
            :profile_times_min_op_times_units_on,
            [
                @expression(
                    model,
                    row.min_operating_point *
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

        # Start up trajectory lower bound
        flow_total = cons.expressions[:outgoing]
        pmin_sum = cons.expressions[:profile_times_min_op_times_units_on]
        start_up_zeroes = cons.expressions[:start_up]
        start_up = []

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

        attach_constraint!(
            model,
            cons,
            table_name,
            [
                if (row.next_SU_start === missing)
                    @constraint(model, 0 == 0)
                else
                    let traj = read_trajectory(row.trajectory),
                        T_su = length(traj),
                        t_start = row.time_block_start,
                        t_end = row.time_block_end,

                        # Should be the start_up time of the next time block
                        start_up_start = row.next_SU_start,

                        sum = sum(
                            collect([
                                if (t_start + i <= start_up_start && start_up_start <= t_end + i)
                                    traj[T_su-i+1]
                                else
                                    0
                                end for i in collect(1:T_su)
                            ]),
                        )

                        @constraint(
                            model,
                            flow_total[row.id] >= pmin_sum[row.id] + start_up[row.id+1] * sum,
                            base_name = "$table_name[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]"
                        )
                    end
                end for row in indices
            ],
        )
    end
    return nothing
end

"""
    add_start_up_trajectory_upper_bound_constraints!(model, constraints)

Adds the start up trajectory upper bound constraints to the model.
"""
function add_start_up_trajectory_upper_bound_constraints!(
    connection,
    model,
    variables,
    expressions,
    constraints,
    profiles,
)
    let table_name = :start_up_trajectory_upper_bound, cons = constraints[table_name]
        indices = _append_data_to_start_up_trajectory(connection, table_name)

        # Expression for maximum available units per year
        expr_avail_simple_method =
            expressions[:available_asset_units_simple_method].expressions[:assets]

        # Expression for Profile * Capacity -> pmax
        attach_expression!(
            cons,
            :profile_times_capacity_times_units_on,
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

        # Start up trajectory upper bound
        flow_total = cons.expressions[:outgoing]
        pmax_sum = cons.expressions[:profile_times_capacity_times_units_on]
        start_up_zeroes = cons.expressions[:start_up]
        start_up = []

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

        attach_constraint!(
            model,
            cons,
            table_name,
            [
                if (row.next_SU_start === missing)
                    @constraint(model, 0 == 0)
                else
                    let traj = read_trajectory(row.trajectory),
                        T_su = length(traj),
                        t_start = row.time_block_start,
                        t_end = row.time_block_end,

                        # Should be the start_up time of the next time block
                        start_up_start = row.next_SU_start,

                        sum = sum(
                            collect([
                                if (t_start + i <= start_up_start && start_up_start <= t_end + i)
                                    traj[T_su-i+1]
                                else
                                    0
                                end for i in collect(1:T_su)
                            ]),
                        )

                        @constraint(
                            model,
                            flow_total[row.id] <= pmax_sum[row.id] + start_up[row.id+1] * sum,
                            base_name = "$table_name[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]"
                        )
                    end
                end for row in indices
            ],
        )
    end
    return nothing
end

function _append_data_to_start_up_trajectory(connection, table_name)
    return DuckDB.query(
        connection,
        "SELECT
            cons.*,
            start_up.time_block_start AS next_SU_start,
            asset.trajectory        AS trajectory,
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
            AND cons.time_block_start >= atr.time_block_start
            AND cons.time_block_end <= atr.time_block_end
            AND cons.rep_period = atr.rep_period
        LEFT JOIN var_start_up AS start_up
            ON cons.asset = start_up.asset
            AND cons.year = start_up.year
            AND cons.rep_period = start_up.rep_period
            AND atr.time_block_end + 1 = start_up.time_block_start
        WHERE asset.investment_method in ('simple', 'none')
        ORDER BY cons.id
        ",
    )
end
