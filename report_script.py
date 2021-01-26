from datetime import datetime
import os
import sys
import shutil



def validate_cont(file, delimiter, csv_fields):
    with open(file, 'r') as file_in:
        rows = file_in.readlines()
        lines_to_evaluated = 0
        total_lines = 0
        for line, r in enumerate(rows):
            total_lines += 1
            split = r.split(delimiter)
            if line_filter(split):
                lines_to_evaluated += 1
                assert len(split) == csv_fields, '%s: not valid record at line: %s' % (file, line + 1)
        print '%s is valid. %s lines of %s are going to be evaluated' % (file, str(lines_to_evaluated), str(total_lines))
        return lines_to_evaluated


def file_to_sorted_generator(file, delimiter):
    with open(file, 'r') as file_in:
        rows = file_in.readlines()
        sorted_rows = sorted(rows, reverse=True)
        return row_generator(sorted_rows, delimiter)


def make_file(file):
    with open(file, 'r') as file_in:
        rows = file_in.readlines()
        sorted_rows = sorted(rows, reverse=True)
        with open(file.replace('.txt', '.csv'), 'w+') as out_f:
            for r in sorted_rows:
                out_f.write(r)


def row_generator(lst, delimiter):
    for i in lst:
        split = i.split(delimiter)
        if line_filter(split):
            yield split


def line_filter(split_line):
    return split_line[3].startswith('/api')


def run(new_input, old_input, delimiter, skip_comparison, csv_fields, output_dir):
    # output files
    directory = os.getcwd() + output_dir
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.mkdir(directory)
    absent = open(directory + "/absent.txt", "w+")
    differences_path = directory + "/differences.txt"
    differences = open(differences_path, "w+")
    WRITE = 'endpoint;requestId;httpMethod;resultCode;resultDescription;clientIp;sessionId;status;numberType;number;paymentType or simType;simId;username;fiscalcode;yearOfBirth;sex;networkStatus;profile;wifiEnriched;allowedMsisdns;residential;installationId;muid;appKey;cid;aid;appId;appPlatform;appVersion;osName;osVersion;tablet;deviceModel;deviceVendor;pushEnabled;pushToken;pushEvent.type;pushEvent.event;pushEvent.treatmentCode;pushEvent.keyCode;pushEvent.campCode;pushEvent.cycle;pushEvent.simId;pushEvent.messageId;registrationStatus;previousAppVersion;consentType;consentValue;productStatisticData.productId;productStatisticData.spyderId;productStatisticData.productType;productStatisticData.objectCode;productStatisticData.productName;productStatisticData.operationType;productStatisticData.activationChain;productStatisticData.fromCampaign;productStatisticData.optinCode;productStatisticData.batch;productStatisticData.type;productStatisticData.conditionCode;productStatisticData.resultCode;callInterceptData.anchor;callInterceptData.policyInterceptId;callInterceptData.policyInterceptName;anchorStatisticsData.anchorDestCode;anchorStatisticsData.anchorOlaId;anchorStatisticsData.anchorViewSuccess;pushEvent.attribution;contactCenterStatisticsData.contactCenterIdrep;\n'

    differences.write(WRITE)

    new_lines = validate_cont(new_input, delimiter, csv_fields)
    old_lines = validate_cont(old_input, delimiter, csv_fields)

    old_generator = file_to_sorted_generator(old_input, delimiter)
    new_generator = file_to_sorted_generator(new_input, delimiter)

    count_old = 0
    count_new = 0
    old_current = None
    new_current = None

    while count_old < old_lines and count_new < new_lines:

        if old_current is None:
            old_current = next(old_generator)
            count_old += 1

        if new_current is None:
            new_current = next(new_generator)
            count_new += 1

        old_id = old_current[0]
        new_id = new_current[0]

        if old_id == new_id:
            at_least_one_diff = False
            diff_field_list = []
            for csv_index, old_csv_el in enumerate(old_current):
                new_csv_el = new_current[csv_index]
                if old_csv_el and old_csv_el != new_csv_el and csv_index not in skip_comparison:
                    diff_field_list.append(" %s != %s" % (old_csv_el, new_csv_el))
                    at_least_one_diff = True
                elif csv_index in skip_comparison:
                    pass
                else:
                    diff_field_list.append("-")
            if at_least_one_diff:
                differences.write("%s;%s;%s\n" % (new_current[3], old_id, ";".join(diff_field_list)))
            new_current = None
            old_current = None
            continue
        # NOT match! on next step ONLY old_row must go ahead
        if old_id > new_id:
            absent.write(old_id + "\n")
            old_current = None
            continue
        # NOT match! on next step ONLY new_row must go ahead
        while new_id > old_id and count_new < new_lines:
            new_current = next(new_generator)
            count_new += 1
            new_id = new_current[0]

    for i in old_generator:
        absent.write(i[0] + '\n')
    absent.close()
    differences.close()

    make_file(differences_path)


def main(argv):
    # CONFIG
    output_dir = '/output'
    csv_fields = 71
    csv_delimiter = ";"
    csv_fields_excluded = [0, 1, 7, 3]  # csv fields position not to be evaluated for comparison

    # input files
    nuovo_report = argv[0]
    vecchio_report = argv[1]

    print '%s %s' % ('Running on PID:', str(os.getpid()))
    start = datetime.today()
    run(nuovo_report, vecchio_report, csv_delimiter, csv_fields_excluded, csv_fields, output_dir)
    print ('It took: ' + str(datetime.today() - start))


if __name__ == "__main__":
   main(sys.argv[1:])
