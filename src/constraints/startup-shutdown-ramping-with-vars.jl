export add_su_sd_ramping_with_vars_constraints!

"""
    add_su_sd_ramping_with_vars_constraints!(model, constraints)

Adds the start-up and shut-down ramping constraints (with start-up and shut-down variables) to the model.
"""
function add_su_sd_ramping_with_vars_constraints!(
    connection,
    model,
    variables,
    expressions,
    constraints,
    profiles,
)
    indices_dict = Dict(
        table_name => _append_su_sd_ramp_vars_data_to_indices(connection, table_name) for
        table_name in (:su_ramp_vars_flow_diff,)
    )

    # expression for p^{availability profile} * p^{capacity}
    # as also found in ramping-and-unit-commitment.jl
    profile_times_capacity = Dict(
        table_name => begin
            indices = indices_dict[table_name]
            [
                _profile_aggregate(
                    profiles.rep_period,
                    (row.profile_name, row.year, row.rep_period),
                    row.time_block_start:row.time_block_end,
                    Statistics.mean,
                    1.0,
                ) * row.capacity for row in indices
            ]
        end for table_name in (:su_ramp_vars_flow_diff,)
    )

    # constraint 13a
    let table_name = :su_ramp_vars_flow_diff, cons = constraints[table_name]
        units_on = cons.expressions[:units_on]
        start_up = cons.expressions[:start_up]
        flow_total = cons.expressions[:outgoing]
        duration = cons.coefficients[:min_outgoing_flow_duration]

        attach_constraint!(
            model,
            cons,
            table_name,
            [
                if row.time_block_start == 1
                    @constraint(model, 0 == 0)
                else
                    p_start_up_ramp = row.max_su_ramp * profile_times_capacity[table_name][row.id]
                    p_ramp_up = row.max_ramp_up * profile_times_capacity[table_name][row.id]
                    p_min = row.min_operating_point * profile_times_capacity[table_name][row.id]

                    @constraint(
                        model,
                        flow_total[row.id] - flow_total[row.id-1] <=
                        start_up[row.id] * (p_start_up_ramp - p_min - p_ramp_up) +
                        units_on[row.id] * (p_min + p_ramp_up * duration[row.id]) -
                        units_on[row.id-1] * p_min,
                        base_name = "$table_name[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]"
                    )
                end for row in indices_dict[table_name]
            ],
        )
    end
end

function _append_su_sd_ramp_vars_data_to_indices(connection, table_name)
    return DuckDB.query(
        connection,
        "SELECT
            cons.*,
            ast_t.capacity,
            ast_t.min_operating_point,
            ast_t.max_ramp_up,
            ast_t.max_ramp_down,
            ast_t.max_su_ramp,
            ast_t.max_sd_ramp,
            assets_profiles.profile_name,
        FROM cons_$table_name AS cons
        LEFT JOIN asset as ast_t
            ON cons.asset = ast_t.asset
        LEFT JOIN assets_profiles
            ON cons.asset = assets_profiles.asset
            AND cons.year = assets_profiles.commission_year
            AND assets_profiles.profile_type = 'availability'
        ORDER BY cons.id
        ",
    )
end
