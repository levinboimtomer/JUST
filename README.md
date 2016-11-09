(WORK IN PROGRESS)
'just' facilitates the construction and management of bash based pipelines on a cluster by 
* Grouping commands under a task name/id
* Sharing global variables among tasks (for example paths to common programs)
* Easily scheduling tasks as jobs on a cluster (job dependence is supported for tasks with consecutive ids)

Reasons to work with just:
* Modularity - 'just' enables writing a modular pipeline (and modularity implies resuable code, shorter debug cycles)
* Reproducibility - Don't struggle with your own commands 3 months from now.
* Slightly better job logs - STDOUT/STDERR are written into files with meaningful names (defined by the task name).


Usage:
* Define a sequence of indexed tasks in a file (here named 'tasks.just'):

<pre>
0:shared_commands:{{
  # anything written here is shared by all tasks at execution time.
  A=1
}}

1:write:{{
  echo $A >> $workdir/1.txt 
  # the variable $A is known here since it is defined in task 0
  # $workdir should be defined by the user at the command line
}}

2:read:{{
  cat $workdir/1.txt
}}
</pre>

* Execute on current machine: just.py tasks.just -s 1-2 --workdir test_just
* Schedule on a cluster: just.py tasks.just -s 1-2 --workdir test_just --q $QUEUE_NAME (e.g. -q '*@@nlp' on ND's CRC)
