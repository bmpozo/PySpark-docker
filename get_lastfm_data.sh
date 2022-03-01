# -- Download lastfm data file
mkdir -p ./data/input
curl http://mtg.upf.edu/static/datasets/last.fm/lastfm-dataset-1K.tar.gz | tar -xz -C ./data/input/