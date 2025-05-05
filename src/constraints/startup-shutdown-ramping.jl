export add_su_sd_ramping_constraints_simple!

"""
    add_su_sd_ramping_constraints_simple!(model, constraints)

Adds the start-up and shut-down ramping constraints to the model.
(11a), (11c)
"""
function add_su_sd_ramping_constraints_simple!(
    connection,
    model,
    variables,
    expressions,
    constraints,
    profiles,
)
    # A way to check if any constraints for this index were defined
    constraintsPresent = false
    for row in constraints[:su_ramping_simple].indices
        constraintsPresent = true
        break
    end

    # If the constraints at this index not defined,
    # this means that no constraints should be added
    if (!constraintsPresent)
        return
    end

    # Warn: runs SQL query. Guarded by `if` statement above
    indices_dict = Dict(
        table_name => _append_su_ramping_data_to_indices(connection, table_name) for
        table_name in (:su_ramping_simple,)
    )

    # Compute ` P^{availability profile} * P^{capacity}`
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
        end for table_name in (:su_ramping_simple,)
    )

    # Start-Up ramping constraint --> (11a)
    let table_name = :su_ramping_simple, cons = constraints[table_name]
        indices = indices_dict[table_name]
        units_on = cons.expressions[:units_on]
        attach_constraint!(
            model,
            cons,
            table_name,
            [
                if row.time_block_start == 1
                    @constraint(model, 0 == 0) # Placeholder for case k = 1
                else
                    @constraint(
                        model,
                        cons.expressions[:outgoing][row.id] -
                        cons.expressions[:outgoing][row.id-1] ≤
                        (
                            row.max_su_ramp * profile_times_capacity[table_name][row.id] +
                            row.max_ramp_up *
                            profile_times_capacity[table_name][row.id] *
                            (min_outgoing_flow_duration - 1)
                        ) * units_on[row.id] -
                        (
                            row.max_su_ramp * profile_times_capacity[table_name][row.id] -
                            row.max_ramp_up * profile_times_capacity[table_name][row.id]
                        ) * units_on[row.id-1],
                        base_name = "su_ramping_simple[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]"
                    )
                end for (row, min_outgoing_flow_duration) in
                zip(indices, cons.coefficients[:min_outgoing_flow_duration])
            ],
        )
    end
end

function _append_su_ramping_data_to_indices(connection, table_name)
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
        LEFT OUTER JOIN assets_profiles
            ON cons.asset = assets_profiles.asset
            AND cons.year = assets_profiles.commission_year
            AND assets_profiles.profile_type = 'availability'
        ORDER BY cons.id
        ",
    )
end
