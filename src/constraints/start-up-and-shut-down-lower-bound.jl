export add_start_up_lower_bound_constraints!

"""
    add_start_up_and_shut_down_lower_bound_constraints!(model, constraints)

Adds the start_up(b) >= units_on(B(b)) - units_on(B(b - 1)) and shut_down(b) >= units_on(B(b - 1)) - units_on(B(b)) constraints to the model.
"""
function add_start_up_and_shut_down_lower_bound_constraints!(
    connection,
    model,
    variables,
    expressions,
    constraints,
)
    let table_name = :start_up_lower_bound, cons = constraints[table_name]
        units_on = cons.expressions[:units_on]
        start_up = cons.expressions[:start_up]

        attach_constraint!(
            model,
            cons,
            table_name,
            [
                begin
                    if row.time_block_start == 1
                        @constraint(model, 0 == 0)
                    else
                        @constraint(
                            model,
                            units_on[row.id] - units_on[row.id-1] <= start_up[row.id],
                            base_name = "$table_name[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]"
                        )
                    end
                end for row in cons.indices
            ],
        )
    end

    let table_name = :shut_down_lower_bound, cons = constraints[table_name]
        shut_down = cons.expressions[:shut_down]
        units_on = cons.expressions[:units_on]

        attach_constraint!(
            model,
            cons,
            table_name,
            [
                begin
                    if row.time_block_start == 1
                        @constraint(model, 0 == 0)
                    else
                        @constraint(
                            model,
                            units_on[row.id-1] - units_on[row.id] <= shut_down[row.id],
                            base_name = "$table_name[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]"
                        )
                    end
                end for row in cons.indices
            ],
        )
    end
end
