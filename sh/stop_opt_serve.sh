#!/bin/bash
ps ax | grep -i 'opt_serve' | grep python | grep -v grep | awk '{print $1}' | xargs kill -SIGTERM


