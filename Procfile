# Use goreman to run: go get github.com/mattn/goreman
master: ./cayley http --config=cayley.cfg.master -v=2 --logtostderr
rep1: sleep 1 && ./cayley http --config=cayley.cfg.replica -v=2 --logtostderr
