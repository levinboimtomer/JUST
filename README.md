(WORK IN PROGRESS)
JUST facilitates working with shell scripts by grouping commands under a task name and allowing to incrementally execute each task.

Usage:
Define a sequence of indexed tasks in a file (for exampletasks.just):

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





