import argparse
import logging
import os

if __name__ == '__main__':

    FORMAT = '[%(asctime)s, %(levelname)s]: %(message)s'
    logging.basicConfig (format=FORMAT)
    logger = logging.getLogger (__name__)
    logger.setLevel (logging.INFO)

    parser = argparse.ArgumentParser ()
    parser.add_argument ('--spark-class', required=True, type=str, help='''Your application's main class
                                                                                (for Java/Scala apps)
                        ''')
    parser.add_argument ('--master', default='yarn', help='''spark://host:port, mesos://host:port, yarn,
                              k8s://https://host:port, or local (Default: local[*]).''')
    parser.add_argument ('--deploy-mode', type=str, default='client', help='''
                                    Whether to launch the driver program locally ("client") or
                                    on one of the worker machines inside the cluster ("cluster")
                                    (Default: client).
                        ''')
    parser.add_argument ('--jars', help='''Comma-separated list of jars to include on the driver
                              and executor classpaths''')
    parser.add_argument ('--files', help='''Comma-separated list of files to be placed in the working
                              directory of each executor. File paths of these files
                              in executors can be accessed via SparkFiles.get(fileName)''')
    parser.add_argument ('--conf', help='Arbitrary Spark configuration property.')
    parser.add_argument ('--driver-java-options', help='Extra Java options to pass to the driver.')
    parser.add_argument ('--driver-memory', help='Memory for driver (e.g. 1000M, 2G) (Default: 1024M).')
    parser.add_argument ('--executor-memory', help='Memory per executor (e.g. 1000M, 2G) (Default: 1G)')
    parser.add_argument ('--executor-cores', help='''Number of cores per executor. (Default: 1 in YARN mode,
                              or all available cores on the worker in standalone mode)''')
    parser.add_argument ('--num-executors', help='''Number of executors to launch (Default: 2).
                              If dynamic allocation is enabled, the initial number of
                              executors will be at least NUM.''')
    parser.add_argument ('--queue', help='''The YARN queue to submit to (Default: "default")''')
    parser.add_argument ('--principal', help='''Principal to be used to login to KDC, while running on
                              secure HDFS.''')
    parser.add_argument ('--keytab', help='''The full path to the file that contains the keytab for the
                              principal specified above. This keytab will be copied to
                              the node running the Application Master via the Secure
                              Distributed Cache, for renewing the login tickets and the
                              delegation tokens periodically.''')
    parser.add_argument ('--custom-args', help='the args for spark application inside.')

    args = parser.parse_args ()

    spark_class = args.spark_class
    master = args.master
    deploy_mode = args.deploy_mode
    jars = args.jars
    files = args.files
    conf = args.conf
    driver_java_options = args.driver_java_options
    driver_memory = args.driver_memory
    executor_memory = args.executor_memory
    executor_cores = args.executor_cores
    num_executors = args.num_executors
    queue = args.queue
    principal = args.principal
    keytab = args.keytab
    custom_args = args.custom_args

    lib = '../../lib/'
    if jars is None:
        jars = ''
        for root, dirs, fls in os.walk (lib):
            for fl in fls:
                jars = jars + lib + fl + ','
    if files is None:
        files = '../../conf/jaas.conf'
    if conf is None:
        conf = '''spark.executor.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf'''
    if driver_java_options is None:
        if deploy_mode == 'client':
            driver_java_options = "-Djava.security.auth.login.config=../../conf/jaas.conf"
        elif deploy_mode == 'cluster':
            driver_java_options = "-Djava.security.auth.login.config=jaas.conf"
        else:
            driver_java_options = ''

    if driver_memory is None:
        driver_memory = '1g'
    if executor_memory is None:
        executor_memory = '1g'
    if executor_cores is None:
        executor_cores = 1
    if num_executors is None:
        num_executors = 2
    if queue is None:
        queue = 'root.users.spark'
    if principal is None:
        principal = 'spark'
    if keytab is None:
        keytab = '/usr/local/keytab/spark.keytab'

    logger.info ('=======submit spark job=========')

    spark_exec = '''
    spark-submit  --class {0} \
    --master {1} \
    --deploy-mode {2} \
    --executor-memory {3} \
    --executor-cores {4} \
    --driver-memory {5} \
    --num-executors {6} \
    --principal {7} \
    --keytab {8} \
    --files {9} \
    --driver-java-options {10} \
    --conf {11} \
    --conf spark.dynamicAllocation.initialExecutors=2 \
    --conf spark.dynamicAllocation.maxExecutors=4 \
    --queue {12} \
    --jars {13} \
    ../../lib/drools-spark-1.0-SNAPSHOT.jar
    '''.format (spark_class, master, deploy_mode, executor_memory, executor_cores, driver_memory, num_executors,
                principal, keytab, files, driver_java_options, conf, queue, jars)

    logger.info ('------spark command:--------\n %s ' % spark_exec)

    os.system (spark_exec)

    # process = subprocess.Popen(exec, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)  # logger.info("===============job start %s=====================" % __name__)  # with process.stdout:  #     for line in iter(process.stdout.readline, b''):  #         logger.info(line.decode().rstrip('\n'))  # exitcode = process.wait()  # 0 means success  # print("exitcode:%d" % exitcode)
