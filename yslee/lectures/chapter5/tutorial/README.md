# Jupyter Notebook Python, Spark Stack

> **Images hosted on Docker Hub are no longer updated. Please, use [quay.io image](https://quay.io/repository/jupyter/pyspark-notebook)**

[![docker pulls](https://img.shields.io/docker/pulls/jupyter/pyspark-notebook.svg)](https://hub.docker.com/r/jupyter/pyspark-notebook/)
[![docker stars](https://img.shields.io/docker/stars/jupyter/pyspark-notebook.svg)](https://hub.docker.com/r/jupyter/pyspark-notebook/)
[![image size](https://img.shields.io/docker/image-size/jupyter/pyspark-notebook/latest)](https://hub.docker.com/r/jupyter/pyspark-notebook/ "jupyter/pyspark-notebook image size")

GitHub Actions in the <https://github.com/jupyter/docker-stacks> project builds and pushes this image to the Registry.

Please visit the project documentation site for help to use and contribute to this image and others.

- [Jupyter Docker Stacks on ReadTheDocs](https://jupyter-docker-stacks.readthedocs.io/en/latest/index.html)
- [Selecting an Image :: Core Stacks :: jupyter/pyspark-notebook](https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html#jupyter-pyspark-notebook)
- [Image Specifics :: Apache Spark](https://jupyter-docker-stacks.readthedocs.io/en/latest/using/specifics.html#apache-spark)

# Files

```shell
.
├── Dockerfile # Docker file for pyspark image
├── README.md
├── chapter5-yslee-spark-jdbc-example.ipynb # Example pyspark jupyter notebook
├── docker-compose.yaml  # docker compose file for pyspark and jdbc(mysql, postgresql) docker
├── ipython_kernel_config.py # jupyter notebook settings
└── scripts # mysql init scripts
    ├── data.sql
    └── schema.sql
```

# How to run

1. Edit and modify args for pyspark for your pyspark version
2. Once you up docker compose then, image will be built if not exists.
3. Check the token and copy to clipboard.
4. Open localhost:10000 in browser, past it to password(token) (If you changed host port then use that port number)
5. Enjoy pyspark jupyter notebook!

```shell
# In present directory, run docker compose
docker-compose up
```
