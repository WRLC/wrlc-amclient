sudo -u archivematica bash -c " \
    set -a -e -x
    source /etc/default/archivematica-dashboard || \
        source /etc/sysconfig/archivematica-dashboard \
            || (echo 'Environment file not found'; exit 1)
    cd /usr/share/archivematica/dashboard
    /usr/share/archivematica/virtualenvs/archivematica/bin/python \
        manage.py rebuild_aip_index_from_storage_service --delete-all
";