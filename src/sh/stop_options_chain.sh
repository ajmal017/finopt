#!/bin/bash
ps ax | grep -i 'options_chain' | grep python | grep -v grep | awk '{print $1}' | xargs kill -SIGTERM


