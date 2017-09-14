# Training related parameters
ALPHA = 0.5
BETA = 0.5
GRAN = 16
GRAN_MAX = 33

# Fold size for learning
FOLD_SIZE = 10

# 1 second window length
T = 1

# Error probability for approximate counts
DELTA = 0.01

BASIC_HEADERS = ['ts', 'ipv4_srcIP', 'sPort', 'ipv4_dstIP', 'dPort', 'nBytes', 'proto', 'tcp_seq', 'tcp_ack',
                 'tcp_flags']

TARGET_SP = 'SPARK'

