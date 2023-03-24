# Instructions

1. Setup a 2x2 grid of terminals.
2. Run the watch script for each directory.
   1. `bash watch.sh data/`
   2. `bash watch.sh output/`
3. Run the generator -> `python generator.py data/`
   1. Change the `--fps` or `--rps` for more computationally difficult examples.
4. Run the pipeline -> `python pipeline.py data/ output/`
