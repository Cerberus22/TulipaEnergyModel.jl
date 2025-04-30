export add_start_up_constraints!

"""
    add_start_up_constraints!(model, constraints)

Adds the start up constraints to the model.
"""
function add_start_up_constraints!(connection, model, variables, expressions, constraints)
    let table_name = :start_up, cons = constraints[:start_up]
        attach_constraint!(
            model,
            cons,
            table_name,
            [
                @constraint(
                    model,
                    start_up <= units_on,
                    base_name = "$table_name[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]"
                ) for (row, start_up, units_on) in
                zip(cons.indices, variables[:start_up].container, variables[:units_on].container)
            ],
        )
    end
end
