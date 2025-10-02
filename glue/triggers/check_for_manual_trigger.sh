#!/bin/bash

aws glue get-trigger --name start_now --query 'Trigger'
