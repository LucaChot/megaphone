import experlib, sys
from executor import execute
from experlib import eprint

eprint("commit: {}".format(experlib.current_commit))

# assumes the scaling code is at this path on the cluster machine(s)
cluster_src_path = "/home/moritzho/Src/dynamic-scaling-mechanism"
cluster_server = "moritzho@sgs-r820-01"

def run_cmd(cmd, redirect=None, background=False):
    full_cmd = "cd {}; {}".format(cluster_src_path, cmd)
    eprint("running: {}".format(full_cmd))
    return execute("ssh -t {} \"{}\"".format(cluster_server, full_cmd) + 
                    (" > {}".format(redirect) if redirect else ""), async=background)

run_cmd("cargo build --release --example word_count")

def word_count_filename(experiment_name, rounds, batch, keys, open_loop, map_mode, n, w):
    return "{}/word_count_n{}_w{}_rounds{}_batch{}_keys{}_{}_{}".format(
        experlib.experdir(experiment_name), n, w, rounds, batch, keys, open_loop, map_mode)

def run_word_count(socket, rounds, batch, keys, open_loop, map_mode, n, p, w, outfile):
    return run_cmd("hwloc-bind socket:{} -- cargo run --release --example word_count -- {} {} {} {} {} -n {} -p {} -w {}".format(socket, rounds, batch, keys, open_loop, map_mode, n, p, w), outfile, True)

all_map_modes = ["sudden", "one-by-one", "fluid"]

def word_count_constant_one_two():
    experiment_name = "word_count-constant-one-two"

    eprint("### {} ###".format(experiment_name))
    eprint(experlib.experdir(experiment_name))
    experlib.ensuredir(experiment_name)

    for batch in [1000000]:
        for keys in [10240000]:
            for map_mode in all_map_modes:
                n = 2
                w = 1
                rounds=10
                open_loop="constant"

                filename = word_count_filename(experiment_name, rounds, batch, keys, open_loop, map_mode, n, w)
                eprint("RUNNING keys: {} in {}".format(keys, filename))
                experlib.waitall([run_word_count(p, rounds, batch, keys, open_loop, map_mode, n, p, w, filename) for p in range(0, 2)])


def word_count_constant_half_all():
    experiment_name = "word_count-constant-half-all"

    eprint("### {} ###".format(experiment_name))
    eprint(experlib.experdir(experiment_name))
    experlib.ensuredir(experiment_name)

    for batch in [1000000]:
        for keys in [40960000]:
            for map_mode in all_map_modes:
                n = 2
                w = 4
                rounds=10
                open_loop="constant"

                filename = word_count_filename(experiment_name, rounds, batch, keys, open_loop, map_mode, n, w)
                eprint("RUNNING keys: {} in {}".format(keys, filename))
                experlib.waitall([run_word_count(p, rounds, batch, keys, open_loop, map_mode, n, p, w, filename) for p in range(0, 2)])

def word_count_square_half_all():
    experiment_name = "word_count-square-half-all"

    eprint("### {} ###".format(experiment_name))
    eprint(experlib.experdir(experiment_name))
    experlib.ensuredir(experiment_name)

    for batch in [2900000]:
        for keys in [20480000 // 2]:
            for map_mode in all_map_modes:
                n = 2
                w = 4
                rounds=20
                open_loop="square"

                filename = word_count_filename(experiment_name, rounds, batch, keys, open_loop, map_mode, n, w)
                eprint("RUNNING keys: {} in {}".format(keys, filename))
                experlib.waitall([run_word_count(p, rounds, batch, keys, open_loop, map_mode, n, p, w, filename) for p in range(0, 2)])

word_count_constant_one_two()
word_count_constant_half_all()
word_count_square_half_all()
