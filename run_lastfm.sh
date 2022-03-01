# -- Download lastfm data file
mkdir -p ./data/input
curl http://mtg.upf.edu/static/datasets/last.fm/lastfm-dataset-1K.tar.gz | tar -xz -C ./data/input/

# -- Run spark job
#docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-src/lastfm/lastfm.py