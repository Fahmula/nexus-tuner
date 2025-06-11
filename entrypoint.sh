#!/bin/sh
gunicorn -w 1 --threads 4 -b 0.0.0.0:80 app:app
