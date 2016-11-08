(WORK IN PROGRESS)
'just' facilitates the construction and management of bash based pipelines on a cluster by 
1. grouping commands under a task name/id and 
2. easily executing tasks by scheduling them on the cluster. job dependence is supported to some extent (chains graphs only)
3. logging STDOUT/STDERR in meaningful names.

Usage:
Define a sequence of indexed tasks in a file (here named 'tasks.just'):

<pre>
1:task1:{{
  echo "$T"; # bash-cmd 1.1 $PARAM_1_1
  echo "2"; # bash-cmd 1.2 $PARAM_1_2
}}

2:task2:{{
  echo 3; # $bash-cmd2 $PARAM2
}}
</pre>

and execute:

just.py tasks.just [config] -s [start[:end]] -workdir:[output_folder]
