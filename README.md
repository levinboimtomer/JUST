(WORK IN PROGRESS)
'just' facilitates the construction and management of bash based pipelines on a cluster by 
* Grouping commands under a task name/id
* Sharing global variables among tasks (for example paths to common programs)
* Easily scheduling tasks as jobs on a cluster (job dependence is supported for tasks with consecutive ids)
* logging STDOUT/STDERR in meaningful file names.

Usage:
* Define a sequence of indexed tasks in a file (here named 'tasks.just'):

<pre>
1:task1:{{
  echo "$T"; # bash-cmd 1.1 $PARAM_1_1
  echo "2"; # bash-cmd 1.2 $PARAM_1_2
}}

2:task2:{{
  echo 3; # $bash-cmd2 $PARAM2
}}
</pre>

* Execute:
just.py tasks.just [config] -s [start[:end]] -workdir:[output_folder]
