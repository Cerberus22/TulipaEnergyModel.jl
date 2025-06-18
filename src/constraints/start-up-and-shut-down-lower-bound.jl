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
    let table_name_su = :start_up_lower_bound,
        cons_su = constraints[:start_up_lower_bound],
        table_name_sd = :shut_down_lower_bound,
        cons_sd = constraints[:shut_down_lower_bound]

        startup_container = []
        shutdown_container = []
        units_on_now_container = []
        last_asset = nothing
        last_rep_period = -1
        for (i, (ind, su, sd, uo)) in enumerate(
            zip(
                variables[:units_on].indices,
                variables[:start_up].container,
                variables[:shut_down].container,
                variables[:units_on].container,
            ),
        )
            if (ind.asset == last_asset && ind.rep_period == last_rep_period)
                push!(startup_container, su)
                push!(shutdown_container, sd)
                push!(units_on_now_container, uo)
            end
            last_asset = ind.asset
            last_rep_period = ind.rep_period
        end

        units_on_prev_container = []

        indices = collect(variables[:units_on].indices)
        container = collect(variables[:units_on].container)

        for i in (1:(length(indices)-1))
            if indices[i].asset == indices[i+1].asset &&
               indices[i].rep_period == indices[i+1].rep_period
                push!(units_on_prev_container, container[i])
            end
        end

        attach_constraint!(
            model,
            cons_su,
            table_name_su,
            [
                @constraint(
                    model,
                    units_on_now - units_on_prev <= start_up,
                    base_name = "$table_name_su[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]"
                ) for (row, start_up, units_on_now, units_on_prev) in zip(
                    cons_su.indices,
                    startup_container,
                    units_on_now_container,
                    units_on_prev_container,
                )
            ],
        )

        attach_constraint!(
            model,
            cons_sd,
            table_name_sd,
            [
                @constraint(
                    model,
                    units_on_prev - units_on_now <= shut_down,
                    base_name = "$table_name_sd[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]"
                ) for (row, shut_down, units_on_now, units_on_prev) in zip(
                    cons_su.indices,
                    shutdown_container,
                    units_on_now_container,
                    units_on_prev_container,
                )
            ],
        )
    end
end
