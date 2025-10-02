#!/bin/bash

aws glue get-crawlers --query "Crawlers[?contains(Name, 'bronze') || contains(Name, 'biz')].[Name,State,Targets]"

