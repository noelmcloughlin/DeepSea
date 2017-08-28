

/srv/salt/ceph/configuration/cache/ceph.conf:
  file.managed:
    - source:
        - salt://ceph/configuration/cache/ceph.conf-shared.j2
    - template: jinja
    - user: salt
    - group: salt
    - mode: 644
    - makedirs: True
    - fire_event: True