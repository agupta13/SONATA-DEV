from utils import *

P4C_BM_SCRIPT = BASE_PATH + "p4c-bmv2/p4c_bm/__main__.py"


def compile_p4_2_json():
    CMD = P4C_BM_SCRIPT + " " + P4_COMPILED + " --json " + JSON_P4_COMPILED
    get_out(CMD)
