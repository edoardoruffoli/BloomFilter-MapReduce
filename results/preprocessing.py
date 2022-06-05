import json


with open('../results/hadoop_results.json') as d:
    data = json.load(d)

    for iter_result in data:
        # For each job stage
        for i in range(3):
            tokens = iter_result['countersJob' + str(i)].split('\t')
            job_stage = tokens[0]
            job_res = {}
            # Create a new json object field for each output result
            for record in tokens:
                tmp = record.split(':')
                if len(tmp) == 1:   # Record with no output result
                    continue
                field_name = ':'.join(tmp[:-1])
                field_value = int(tmp[-1])
                job_res[field_name] = field_value

            iter_result[job_stage] = job_res

            # Remove the string containing the output of the stage
            del iter_result['countersJob' + str(i)]

with open('../results/hadoop_results_final.json', 'w') as fp:
    json.dump(data, fp, indent=4)