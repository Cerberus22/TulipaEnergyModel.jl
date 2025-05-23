using Pkg
Pkg.activate(".")
Pkg.instantiate()

using BenchmarkTools
using TulipaEnergyModel
using TulipaIO
using DuckDB

# case_studies_to_run = ["1hr","1hrmin","2hr","2hrmin","4hr","4hrmin","6hr","6hrmin","8hr","8hrmin"]
case_studies_to_run = ARGS

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
    end samples = 5 evals = 1 seconds = 86400 setup = (energy_problem = EnergyProblem(input_setup($input_folder)))

    # Create a model for solving
    model_to_solve = create_model!(EnergyProblem(input_setup(input_folder)))

    key = "$case"
    # Benchmark of running the model
    SUITE["run_model"]["$case"] = @benchmarkable begin
        solve_model!(energy_problem)
    end samples = 5 evals = 1 seconds = 86400 setup = (energy_problem = $model_to_solve) teardown = (global energy_problem_solved; energy_problem_solved[$key] = energy_problem)
end

results_of_run = run(SUITE, verbose=true)

# Save run times
BenchmarkTools.save("debugging\\results\\output.json", results_of_run)

# Save optimal solution
for (key, value) in energy_problem_solved
    debugFolder = joinpath(pwd(), "debugging\\results")
    exportFolder = mkpath(joinpath(debugFolder, key))
    save_solution!(value)
    export_solution_to_csv_files(exportFolder, value)
    filePath = joinpath(exportFolder, "objective_value.txt")
    write(filePath, string(value.objective_value))
end
