export add_su_ramp_vars_flow_diff_constraints!

"""
    add_su_ramp_vars_flow_diff_constraints!(model, constraints)

Adds the start-up ramping flow difference constraints (with start-up variables) to the model.
"""
function add_su_ramp_vars_flow_diff_constraints!(
    connection,
    model,
    variables,
    expressions,
    constraints,
    profiles,
)

    # do not run the SQL query below if these constraints are not used,
    # as that will result in non-existing columns being searched for
    if (length(collect(constraints[:su_ramp_vars_flow_diff].indices)) == 0)
        return
    end

    indices_dict = Dict(
        table_name => _append_su_ramp_vars_data_to_indices(connection, table_name) for
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

        attach_constraint!(
            model,
            cons,
            table_name,
            [
                if row.time_block_start == 1
                    @constraint(model, 0 == 0)
                else
                    @constraint(
                        model,
                        flow_total[row.id] - flow_total[row.id-1] <=
                        (
                            row.max_su_ramp * profile_times_capacity[table_name][row.id]                # p^{start up ramp}
                            -
                            row.min_operating_point * profile_times_capacity[table_name][row.id]        # p^{min}
                            -
                            row.max_ramp_up * profile_times_capacity[table_name][row.id]                # p^{ramp up}
                        ) * start_up[row.id] +
                        (
                            row.min_operating_point * profile_times_capacity[table_name][row.id]        # p^{min}
                            +
                            row.max_ramp_up * profile_times_capacity[table_name][row.id] * duration     # p^{ramp up} * duration
                        ) * units_on[row.id] -
                        (
                            row.min_operating_point * profile_times_capacity[table_name][row.id]        # p^{min}
                        ) * units_on[row.id-1],
                        base_name = "$table_name[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]"
                    )
                end for (row, duration) in
                zip(indices_dict[table_name], cons.coefficients[:min_outgoing_flow_duration])
            ],
        )
    end
end

function _append_su_ramp_vars_data_to_indices(connection, table_name)
    return DuckDB.query(
        connection,
        "SELECT
            cons.*,
            ast_t.capacity,
            ast_t.min_operating_point,
            ast_t.max_ramp_up,
            ast_t.max_ramp_down,
            ast_t.max_su_ramp,
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
