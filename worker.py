import pika
from pika.exceptions import ConnectionClosed
import json
import subprocess as sp
import shutil
import errno
import os
from mongoengine import connect, Document, IntField, StringField, BooleanField, DateTimeField
import argparse
import datetime
from utils import aggregate

parser = argparse.ArgumentParser()
parser.add_argument("--masterip", help="The IP of the master, if not in local pod", type=str)
args = parser.parse_args()

DATA_TOP = '/data'
MRB_TOP = '/products/dev'
LOG_TOP = '/data/logs'
LINK_PATH = '/data/docker_user/out'
DB = 'lariatsoft'

TEST = False


class Batch(Document):
    batch_id = IntField(required=True)
    job_id = IntField(required=True)
    events = IntField()
    out_path = StringField()
    log_path = StringField()
    err_path = StringField()
    status = StringField()
    complete = BooleanField(default=False)
    error = StringField()
    start_time = DateTimeField()
    end_time = DateTimeField()

    def __str__(self):
        return str(self.batch_id)


def callback(ch, method, properties, body):
    data_dict = json.loads(body)
    fickle_file = os.path.join(MRB_TOP, 'MC_genie_numu_CC_seed-service.fcl')
    print data_dict

    job_id = str(data_dict.get('job_id'))
    batch_id = str(data_dict.get('batch_id'))
    batch_object = Batch.objects.get(job_id=job_id, batch_id=batch_id)

    if not job_id or not batch_id or not batch_object:
        print('No id!')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    print("<received_job>\n <job_id>   {0}\n "
          "<batch_id> {1}\n".format(job_id, batch_id))
    # Create log and error file
    if data_dict.get('log_dir'):
        log_path = os.path.join(LOG_TOP, data_dict.get('log_dir'))
    else:
        log_path = os.path.join(LOG_TOP, job_id)
    mkdir_p(log_path)
    log_file_path = os.path.join(log_path, str(batch_id) + '.log')
    err_file_path = os.path.join(log_path, 'err_' + str(batch_id) + '.log')
    log_file = open(log_file_path, 'w')
    err_file = open(err_file_path, 'w')
    batch_object.log_path = log_file_path
    batch_object.err_path = err_file_path
    batch_object.start_time = datetime.datetime.now()

    # if new file, copy new file to MRB_TOP DATA_TOP + new_file
    try:
        if data_dict.get('new_file'):
            new_file = str(data_dict.get('new_file')).replace(" ", "")
            new_file_path = os.path.join(DATA_TOP, new_file)
            shutil.copy(new_file_path, MRB_TOP)
            print >> log_file, "<new_file> '{0}' copied to $MRB_TOP".format(new_file)
            fickle_file = os.path.join(MRB_TOP, new_file.split('/')[-1])

        # check out_path exists, if exist cd, else make and cd, if no exist then make id dir
        # $DATA_TOP/out_dir/id
        if data_dict.get('out_dir'):
            out_path = os.path.join(DATA_TOP, data_dict.get('out_dir'), job_id)
            if not os.path.exists(out_path):
                mkdir_p(out_path)
            os.chdir(out_path)
        else:
            out_path = os.path.join(DATA_TOP, job_id)
            mkdir_p(out_path)
            os.chdir(out_path)
        batch_object.out_path = out_path
        batch_object.save()

    except Exception as e:
        update_error(batch_id, job_id, e)
        print >> err_file, '<error> {0}'.format(e)
        print(e)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    exec_list = [
        'lar -c {0} -n {1} -o single_gen_{2}.root'.format(fickle_file, data_dict.get('events'),
                                                          batch_id),
        'lar -c /products/dev/WireDump_numu_NC-1.fcl -s single_gen_{0}.root -T wire_dump_out_{0}.root'.format(
            batch_id),
        'python /products/dev/ProcessRootFile.py ./wire_dump_out_{0}.root out_{0}'.format(batch_id)]

    print >> log_file, '<working dir> {0}'.format(out_path)

    for cmd in exec_list:
        print >> log_file, '<cmd> {0}'.format(cmd)
        try:
            update_status(batch_id, job_id, cmd)
            sp.call(cmd, shell=True, stdout=log_file, stderr=err_file)
        except Exception as e:
            print(e)
            print >> err_file, '<error> {0}'.format(e)
            update_error(batch_id, job_id, e)

    # Change permissions
    for r, d, f in os.walk(os.getcwd()):
        os.chmod(r, 0777)

    # Aggregate output
    aggregate(out_path, LINK_PATH)

    print "<job end>"
    print >> log_file, "<job end>"
    log_file.close()
    err_file.close()
    update_complete(batch_id, job_id)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    return


def update_error(batch_id, job_id, error):
    batch_object = Batch.objects.get(job_id=job_id, batch_id=batch_id)
    if batch_object.error:
        batch_object.error += '<error> {0}\n'.format(error)
    else:
        batch_object.error = '<error> {0}\n'.format(error)
    batch_object.save()


def update_complete(batch_id, job_id):
    batch_object = Batch.objects.get(job_id=job_id, batch_id=batch_id)
    batch_object.complete = True
    batch_object.end_time = datetime.datetime.now()
    batch_object.save()


def update_status(batch_id, job_id, status):
    batch_object = Batch.objects.get(job_id=job_id, batch_id=batch_id)
    batch_object.status = status
    batch_object.save()


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def start():
    if args.masterip:
        MASTER_IP = args.masterip

    elif not TEST:
        try:
            with open('/etc/hosts') as hosts_file:
                for line in hosts_file:
                    if line.split()[1] == 'master':
                        print 'master_ip = {0}'.format(line.split()[0])
                        MASTER_IP = line.split()[0]

                if not MASTER_IP:
                    print('can not find master ip in hosts!')

        except Exception as e:
            print('can not find master ip in hosts!')

    else:
        MASTER_IP = 'localhost'

    print('<MASTER_IP> {0}'.format(MASTER_IP))
    # If connection issue, reconnect

    try:
        # MongoDB
        db = connect(host='mongodb://{0}/{1}'.format(MASTER_IP, DB))

        # Pika ==> RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters(MASTER_IP))
        channel = connection.channel()
        channel.queue_declare(queue='dispatch')
        channel.basic_qos(prefetch_count=1)
        worker_id = channel.basic_consume(callback, queue='dispatch')
        print '<worker_id> {0}'.format(worker_id)
        print(' [*] Waiting for tasks. To exit press CTRL+C')
        channel.start_consuming()

    except ConnectionClosed as e:
        print(e)
        start()

    except Exception as e:
        print(e)


if __name__ == '__main__':
    start()
