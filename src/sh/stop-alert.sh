#!/bin/bash
ps ax | grep -i 'alert' | grep python | grep -v grep | awk '{print $1}' | xargs kill -SIGTERM


