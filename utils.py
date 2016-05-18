import os
import sys

OUT_PATH = '/data/docker_user/out'

def aggregate(dirs, out_path):
    os.chdir(dirs)

    cur_dir = os.getcwd()
    print('<<cur_dir>> {0}'.format(cur_dir))

    particle = cur_dir.split('/')[-2]
    job_id = cur_dir.split('/')[-1]

    os.mkdir(os.path.join(out_path, particle))

    print '<<particle>> {0}'.format(particle)
    print '<<job_id>> {0}'.format(job_id)
    for item in os.listdir(cur_dir):
        if os.path.isdir(item) and 'out' in item:
            top_path = os.path.join(cur_dir, item, 'h5')
            print '<<top_path>> {0}'.format(top_path)
            for thing in os.listdir(top_path):
                tmp_out = os.path.join(out_path, particle)

                new_file_name = '_'.join([job_id, thing])

                link_path = os.path.join(tmp_out, new_file_name)
                h5_path = os.path.join(top_path, thing)
                print '<<h5_path>> {0}'.format(h5_path)
                print '<<link_path>> {0}'.format(link_path)
                os.symlink(h5_path, link_path)
    return


if __name__ == '__main__':
    aggregate(sys.argv[1], out_path=OUT_PATH)
