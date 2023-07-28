#!/usr/bin/python3

import os
import functools
import pandas
from optparse import OptionParser

outfile="/tmp/rel_task.csv"

relevant_indexes = ['pid', 'comm', 'sum_exec_runtime', 'wait_sum', 'sum_sleep_runtime', 'nr_switches', 'nr_voluntary_switches', 'nr_involuntary_switches', 'wait_count', 'nr_migrations', 'nr_wakeups', 'nr_failed_migrations_affine', 'nr_failed_migrations_hot', 'nr_failed_migrations_running', 'nr_forced_migrations', 'nr_migrations_cold', 'nr_wakeups_affine', 'nr_wakeups_affine_attempts', 'nr_wakeups_idle', 'nr_wakeups_local', 'nr_wakeups_migrate', 'nr_wakeups_passive', 'nr_wakeups_remote', 'nr_wakeups_sync']

out_dict = {}
index_dict = {}

def summarize_task_class(df):
    data_indices = ['nr_instances'] + relevant_indexes[2:len(relevant_indexes)]
    for i in range(len(data_indices)):
        index_dict[data_indices[i]] = i
                   
    
    for index in range(len(df)):
        task_list = df.loc[index].to_list()
        taskname= task_list[1]
        data_list = [1] + task_list[2:len(task_list)]
        try:
            l1 = out_dict[taskname]
            l2 = data_list
            lsum = [x + y for (x, y) in zip(l1, l2)]
            out_dict[taskname] = lsum
        except KeyError:
            out_dict[taskname] = data_list

    for taskname in out_dict.keys():
        avg_atom = avg_cpu = avg_wait = avg_idle = 0
        this_list = out_dict[taskname]

        nr_instances = this_list[index_dict['nr_instances']]
        nr_switches = this_list[index_dict['nr_switches']]
        nr_migrations = this_list[index_dict['nr_migrations']]
        wait_count = this_list[index_dict['wait_count']]
        nr_voluntary_switches = this_list[index_dict['nr_voluntary_switches']]
        
        runtime = this_list[index_dict['sum_exec_runtime']]
        wait_sum = this_list[index_dict['wait_sum']]
        sleeptime = this_list[index_dict['sum_sleep_runtime']]

        if nr_switches:
            avg_atom = runtime / nr_switches

        if nr_migrations:
            avg_cpu = runtime / nr_migrations

        if wait_count:
            avg_wait = wait_sum / wait_count

        if nr_voluntary_switches:
            avg_idle = sleeptime / nr_voluntary_switches

        avg_list = [x/nr_instances for x in this_list[1:len(this_list)]]
        sum_avg_list = functools.reduce(lambda a, b: a + b, [[x, y] for (x, y) in zip(this_list[1:len(this_list)], avg_list)])
        
        newlist = this_list[0:1] + [avg_atom, avg_cpu, avg_wait, avg_idle] + sum_avg_list
        #newlist.extend(this_list)
        out_dict[taskname] = newlist

    sum_avg_indices = []

    sum_avg_indices = functools.reduce(lambda a, b: a + b, [[x, y] for (x, y) in [('sum of ' + x, 'avg of ' + x) for x in data_indices[1:len(data_indices)]]])                                    
    # for x in data_indices[1:len(data_indices)]:
    #     sum_avg_indices.append('sum of ' + x)
    #     sum_avg_indices.append('avg of ' + x)
    new_indices = data_indices[0:1] + ['avg_atom', 'avg_cpu', 'avg_wait', 'avg_idle'] + sum_avg_indices
    out_df = pandas.DataFrame.from_dict(out_dict, orient='index', columns=new_indices)
    out_df.sort_index(inplace=True)
    return out_df
    
def parse_pertask_class_summary(filepath):
    tasks_df = pandas.read_csv(filepath)

    rel_df = tasks_df[relevant_indexes]
    out_df = summarize_task_class(rel_df)

    csvfilename = outfile
    out_df.to_csv(csvfilename, sep=",", header=True, index=True)
    print("Output written to: %s" %(csvfilename))



if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-i", "--infile", dest="in_file", type=str, help="path to the taskstat report csv")
    parser.add_option("-o", "--outfile", dest="out_file", type=str, help="Output file")

    (options, args) = parser.parse_args()

    taskstat_csv = options.in_file

    if options.out_file != None:
        outfile = options.out_file

    if (not os.path.exists(taskstat_csv)) or (not os.path.isfile(taskstat_csv)):
        print("%s is not a valid input file" %(taskstat_csv))
        os.exit(1)

    parse_pertask_class_summary(taskstat_csv)
    
