using BenchmarkTools
using TulipaEnergyModel
using TulipaIO
using DuckDB
using JuMP

case_studies_to_run = ["1hr","1hrmin","2hr","2hrmin","4hr","4hrmin","6hr","6hrmin","8hr","8hrmin","geographical","geographicalmin"]

# DB connection helper
function input_setup(input_folder)
    connection = DBInterface.connect(DuckDB.DB)
    TulipaIO.read_csv_folder(
        connection,
        input_folder;
        schemas = TulipaEnergyModel.schema_per_table_name,
    )
    return connection
end

function NL_cost(connection)
    indices = DuckDB.query(
        connection,
        "SELECT
            var.id,
            t_objective_assets.weight_for_asset_investment_discount
                * t_objective_assets.investment_cost
                * t_objective_assets.capacity
                AS cost,
        FROM var_assets_investment AS var
        LEFT JOIN t_objective_assets
            ON var.asset = t_objective_assets.asset
            AND var.milestone_year = t_objective_assets.milestone_year
        WHERE var.asset LIKE 'NL%'
        ORDER BY
            var.id
        ",
    )

    assets_investment_vars = DuckDB.query(
        connection,
        "SELECT asset, solution FROM var_assets_investment WHERE asset LIKE 'NL%' ORDER BY id",
    )

    assets_investment_cost =
        sum(row.cost * var.solution for (row, var) in zip(indices, assets_investment_vars))

    indices = DuckDB.query(
        connection,
        "SELECT
            var.id,
            t_objective_assets.weight_for_asset_investment_discount
                * t_objective_assets.investment_cost_storage_energy
                * t_objective_assets.capacity_storage_energy
                AS cost,
        FROM var_assets_investment_energy AS var
        LEFT JOIN t_objective_assets
            ON var.asset = t_objective_assets.asset
            AND var.milestone_year = t_objective_assets.milestone_year
        WHERE var.asset LIKE 'NL%'
        ORDER BY
            var.id
        ",
    )

    storage_assets_energy_investment_vars = DuckDB.query(
        connection,
        "SELECT solution FROM var_assets_investment_energy WHERE asset LIKE 'NL%' ORDER BY id",
    )

    storage_assets_energy_investment_cost = sum(
        row.cost * variable.solution for
        (row, variable) in zip(indices, storage_assets_energy_investment_vars)
    )

    indices = DuckDB.query(
        connection,
        "SELECT
            var.id,
            t_objective_flows.weight_for_operation_discounts
                * rpinfo.weight_sum
                * rpinfo.resolution
                * (var.time_block_end - var.time_block_start + 1)
                * t_objective_flows.variable_cost
                AS cost,
        FROM var_flow AS var
        LEFT JOIN t_objective_flows
            ON var.from_asset = t_objective_flows.from_asset
            AND var.to_asset = t_objective_flows.to_asset
            AND var.year = t_objective_flows.milestone_year
        LEFT JOIN (
            SELECT
                rpmap.year,
                rpmap.rep_period,
                SUM(weight) AS weight_sum,
                ANY_VALUE(rpdata.resolution) AS resolution
            FROM rep_periods_mapping AS rpmap
            LEFT JOIN rep_periods_data AS rpdata
                ON rpmap.year=rpdata.year AND rpmap.rep_period=rpdata.rep_period
            GROUP BY rpmap.year, rpmap.rep_period
        ) AS rpinfo
            ON var.year = rpinfo.year
            AND var.rep_period = rpinfo.rep_period
        WHERE ((var.to_asset LIKE 'NL%') OR (var.from_asset LIKE 'NL%'))
        ORDER BY var.id
        ",
    )

    flow_vars = DuckDB.query(
        connection,
        "SELECT solution FROM var_flow WHERE ((to_asset LIKE 'NL%') OR (from_asset LIKE 'NL%')) ORDER BY id",
    )

    flows_variable_cost = sum(row.cost * flow.solution for (row, flow) in zip(indices, flow_vars))

    indices = DuckDB.query(
        connection,
        "SELECT
            var.id,
            t_objective_assets.weight_for_operation_discounts
                * rpinfo.weight_sum
                * rpinfo.resolution
                * (var.time_block_end - var.time_block_start + 1)
                * t_objective_assets.units_on_cost
                AS cost,
        FROM var_units_on AS var
        LEFT JOIN t_objective_assets
            ON var.asset = t_objective_assets.asset
            AND var.year = t_objective_assets.milestone_year
        LEFT JOIN (
            SELECT
                rpmap.year,
                rpmap.rep_period,
                SUM(weight) AS weight_sum,
                ANY_VALUE(rpdata.resolution) AS resolution
            FROM rep_periods_mapping AS rpmap
            LEFT JOIN rep_periods_data AS rpdata
                ON rpmap.year=rpdata.year AND rpmap.rep_period=rpdata.rep_period
            GROUP BY rpmap.year, rpmap.rep_period
        ) AS rpinfo
            ON var.year = rpinfo.year
            AND var.rep_period = rpinfo.rep_period
        WHERE t_objective_assets.units_on_cost IS NOT NULL AND (var.asset LIKE 'NL%')
        ORDER BY var.id
        ",
    )

    units_on_vars = DuckDB.query(
        connection,
        "SELECT asset, solution FROM var_units_on WHERE asset LIKE 'NL%' ORDER BY id",
    )

    units_on_cost =
        sum(row.cost * units_on.solution for (row, units_on) in zip(indices, units_on_vars))

    full_cost =
        units_on_cost +
        flows_variable_cost +
        assets_investment_cost +
        storage_assets_energy_investment_cost

    operational_cost = units_on_cost + flows_variable_cost

    return full_cost, operational_cost
end

function non_NL_cost(connection)
    indices = DuckDB.query(
        connection,
        "SELECT
            var.id,
            t_objective_assets.weight_for_asset_investment_discount
                * t_objective_assets.investment_cost
                * t_objective_assets.capacity
                AS cost,
        FROM var_assets_investment AS var
        LEFT JOIN t_objective_assets
            ON var.asset = t_objective_assets.asset
            AND var.milestone_year = t_objective_assets.milestone_year
        WHERE var.asset NOT LIKE 'NL%'
        ORDER BY
            var.id
        ",
    )

    assets_investment_vars = DuckDB.query(
        connection,
        "SELECT asset, solution FROM var_assets_investment WHERE asset NOT LIKE 'NL%' ORDER BY id",
    )

    assets_investment_cost =
        sum(row.cost * var.solution for (row, var) in zip(indices, assets_investment_vars))

    indices = DuckDB.query(
        connection,
        "SELECT
            var.id,
            t_objective_assets.weight_for_asset_investment_discount
                * t_objective_assets.investment_cost_storage_energy
                * t_objective_assets.capacity_storage_energy
                AS cost,
        FROM var_assets_investment_energy AS var
        LEFT JOIN t_objective_assets
            ON var.asset = t_objective_assets.asset
            AND var.milestone_year = t_objective_assets.milestone_year
        WHERE var.asset NOT LIKE 'NL%'
        ORDER BY
            var.id
        ",
    )

    storage_assets_energy_investment_vars = DuckDB.query(
        connection,
        "SELECT solution FROM var_assets_investment_energy WHERE asset NOT LIKE 'NL%' ORDER BY id",
    )

    storage_assets_energy_investment_cost = sum(
        row.cost * variable.solution for
        (row, variable) in zip(indices, storage_assets_energy_investment_vars)
    )

    indices = DuckDB.query(
        connection,
        "SELECT
            var.id,
            t_objective_flows.weight_for_operation_discounts
                * rpinfo.weight_sum
                * rpinfo.resolution
                * (var.time_block_end - var.time_block_start + 1)
                * t_objective_flows.variable_cost
                AS cost,
        FROM var_flow AS var
        LEFT JOIN t_objective_flows
            ON var.from_asset = t_objective_flows.from_asset
            AND var.to_asset = t_objective_flows.to_asset
            AND var.year = t_objective_flows.milestone_year
        LEFT JOIN (
            SELECT
                rpmap.year,
                rpmap.rep_period,
                SUM(weight) AS weight_sum,
                ANY_VALUE(rpdata.resolution) AS resolution
            FROM rep_periods_mapping AS rpmap
            LEFT JOIN rep_periods_data AS rpdata
                ON rpmap.year=rpdata.year AND rpmap.rep_period=rpdata.rep_period
            GROUP BY rpmap.year, rpmap.rep_period
        ) AS rpinfo
            ON var.year = rpinfo.year
            AND var.rep_period = rpinfo.rep_period
        WHERE NOT ((var.to_asset LIKE 'NL%') OR (var.from_asset LIKE 'NL%'))
        ORDER BY var.id
        ",
    )

    flow_vars = DuckDB.query(
        connection,
        "SELECT solution FROM var_flow WHERE NOT ((to_asset LIKE 'NL%') OR (from_asset LIKE 'NL%')) ORDER BY id",
    )

    flows_variable_cost = sum(row.cost * flow.solution for (row, flow) in zip(indices, flow_vars))

    indices = DuckDB.query(
        connection,
        "SELECT
            var.id,
            t_objective_assets.weight_for_operation_discounts
                * rpinfo.weight_sum
                * rpinfo.resolution
                * (var.time_block_end - var.time_block_start + 1)
                * t_objective_assets.units_on_cost
                AS cost,
        FROM var_units_on AS var
        LEFT JOIN t_objective_assets
            ON var.asset = t_objective_assets.asset
            AND var.year = t_objective_assets.milestone_year
        LEFT JOIN (
            SELECT
                rpmap.year,
                rpmap.rep_period,
                SUM(weight) AS weight_sum,
                ANY_VALUE(rpdata.resolution) AS resolution
            FROM rep_periods_mapping AS rpmap
            LEFT JOIN rep_periods_data AS rpdata
                ON rpmap.year=rpdata.year AND rpmap.rep_period=rpdata.rep_period
            GROUP BY rpmap.year, rpmap.rep_period
        ) AS rpinfo
            ON var.year = rpinfo.year
            AND var.rep_period = rpinfo.rep_period
        WHERE t_objective_assets.units_on_cost IS NOT NULL AND NOT (var.asset LIKE 'NL%')
        ORDER BY var.id
        ",
    )

    units_on_vars = DuckDB.query(
        connection,
        "SELECT asset, solution FROM var_units_on WHERE asset NOT LIKE 'NL%' ORDER BY id",
    )

    units_on_cost =
        sum(row.cost * units_on.solution for (row, units_on) in zip(indices, units_on_vars))

    full_cost =
        units_on_cost +
        flows_variable_cost +
        assets_investment_cost +
        storage_assets_energy_investment_cost

    operational_cost = units_on_cost + flows_variable_cost

    return full_cost, operational_cost
end

global energy_problem_solved = Dict()

# CREATE THE BENCHMARK SUITE
const SUITE = BenchmarkGroup()
SUITE["create_model"] = BenchmarkGroup()
SUITE["run_model"] = BenchmarkGroup()

for case in case_studies_to_run
    input_folder = joinpath(pwd(), "debugging\\Experiment\\$case")

    # Benchmark of creating the model
    SUITE["create_model"]["$case"] = @benchmarkable begin
        create_model!(energy_problem)
    end samples = 10 evals = 1 seconds = 86400 setup = (energy_problem = EnergyProblem(input_setup($input_folder)))

    key = "$case"
    # Benchmark of running the model
    SUITE["run_model"]["$case"] = @benchmarkable begin
        solve_model!(energy_problem)
    end samples = 10 evals = 1 seconds = 86400 setup = (energy_problem = create_model!(EnergyProblem(input_setup($input_folder)))) teardown = (global energy_problem_solved; energy_problem_solved[$key] = energy_problem)
end

results_of_run = run(SUITE, verbose=true)

# Save run times
BenchmarkTools.save("debugging\\results\\output.json", results_of_run)

# Save optimal solution
for (key, value) in energy_problem_solved
    debugFolder = joinpath(pwd(), "debugging\\results")
    exportFolder = mkpath(joinpath(debugFolder, key))

    # Variable Tables
    save_solution!(value)
    export_solution_to_csv_files(exportFolder, value)

    # Objective Value
    objValFile = joinpath(exportFolder, "objective_value.txt")

    NL_total_cost, NL_op_cost = NL_cost(value.db_connection)
    non_NL_total_cost, non_NL_op_cost = non_NL_cost(value.db_connection)

    write(
        objValFile,
        string(value.objective_value) *
        ";" *
        string(NL_total_cost) *
        ";" *
        string(NL_op_cost) *
        ";" *
        string(non_NL_total_cost) *
        ";" *
        string(non_NL_op_cost),
    )

    # Model.lp
    modelLpFile = joinpath(exportFolder, "model.lp")
    JuMP.write_to_file(value.model, modelLpFile)
end
