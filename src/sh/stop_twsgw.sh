#!/bin/bash
ps ax | grep -i 'tws_gateway' | grep python | grep -v grep | awk '{print $1}' | xargs kill -SIGTERM


