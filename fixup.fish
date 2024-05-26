#!/usr/bin/env fish
set epoch (head -n 1 (tail -n +$argv[1] brokenData.txt | psub) | string split -m 1 -f 2 ': ')
env NUM_EPOCH_TASKS=1 FIXUP_EPOCHS=$epoch FIXUP_VALIDATORS=(grep ": $epoch" brokenData.txt | string sub -e -(math (string length $epoch) + 2) | psub -s .txt) node cacher
