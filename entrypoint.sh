#!/bin/sh

exit_code=1  # Default to error if the python script has not yet run
pid_to_kill=$$  # Default to the current process

# Shell commands cannot be interupted so if long running it needs to run in the background and use: wait $!
trap 'kill -TERM ${pid_to_kill}; wait ${pid_to_kill}; exit ${exit_code}' TERM
trap 'kill -INT ${pid_to_kill}; wait ${pid_to_kill}; exit ${exit_code}' INT

# Ensure this function is used for long running tasks so that the signal trap can be used
run_signal_aware() {
    "$@" &
    pid_to_kill=$!
    wait $pid_to_kill
    pid_to_kill=$$
}

run_signal_aware gunicorn -w 1 --threads 4 -b "0.0.0.0:${NEXUS_PORT}" app:app
exit_code=$?

echo "Exiting with code ${exit_code}"
exit ${exit_code}
