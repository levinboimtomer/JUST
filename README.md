define a sequence of indexed tasks in a file called tasks.just:

1:task1:{{
  echo "$T"; # bash-cmd11 $PARAM_1_1
  echo "2"; # bash-cmd12 $PARAM_1_2
}}

2:task2:{{
  echo 3; # $bash-cmd2 $PARAM2
}}

and execute:
just.py tasks.just [config] -s [start[:end]] -workdir:[output_folder]





