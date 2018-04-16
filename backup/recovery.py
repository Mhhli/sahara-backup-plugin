from sahara_dashboard.api import sahara as sahara
import utils as u
from openstack_dashboard.api import nova
from openstack_dashboard.api import glance
import time
import io
from oslo_utils import uuidutils


def recovery(request, path, net_id, name=None, key_pair=None, security_group=None, flavor_id=None):
    is_file_exist(path)
    cluster, ct, ngs, flavors, keypair, sgs, images = load_backup_file(path)
    if key_pair is None:
        keypair = recovery_keypair(request, keypair)
    if flavor_id is None:
        flavors = recovery_flavors(request, flavors)
    if security_group is None:
        sgs = recovery_sgs(request, sgs)
    if name is None:
        name = cluster['name'] + '-' + uuidutils.generate_uuid()[0:7]
    instances = get_instances_ngs(cluster)
    ri = recovery_images(request, images, instances)
    instances = recovery_instance(path, instances)
    ngs = recovery_ngs(request, ngs, instances, flavor_id, security_group, flavors, sgs)
    ct = recovery_ct(request, ct, ngs)
    cluster = recovery_cluster(request, ct, cluster, net_id, name, keypair['name'], instances[0]['image_id'])
    return cluster


file_name = ['cluster.json', 'clusterTemplate.json', 'node_groups.json', 'flavors.json',
             'keypair.json', 'security_groups.json', 'images.json']


def recovery_cluster(request, ct, cluster, net_id, name, keypair, default_image_id):
    cluster = sahara.cluster_create(request,
                                    name=name,
                                    plugin_name=cluster['plugin_name'],
                                    hadoop_version=cluster['hadoop_version'],
                                    cluster_template_id=ct['id'],
                                    is_transient=cluster['is_transient'],
                                    description=cluster['description'] + "backup",
                                    cluster_configs=cluster['cluster_configs'],
                                    user_keypair_id=keypair,
                                    anti_affinity=cluster['anti_affinity'],
                                    default_image_id=default_image_id,
                                    net_id=net_id,
                                    count=1,
                                    use_autoconfig=cluster['use_autoconfig'],
                                    is_public=cluster['is_public'],
                                    is_protected=cluster['is_protected']
                                    ).to_dict()
    cluster = sahara.cluster_get(request, cluster['id']).to_dict()
    return cluster


def delete_flavor(request, flavors):
    for flavor in flavors:
        nova.flavor_delete(request, flavor['id'])


def delete_keypair(request, keypair):
    nova.keypair_delete(request, keypair['name'])


def delete_sgs(request, sgs):
    sg_manager = nova.SecurityGroupManager(request)
    for sg in sgs:
        sg_manager.delete(sg['id'])


def delete_ngs(request, ngs):
    for ng in ngs:
        sahara.nodegroup_template_delete(request, ng['id'])


def delete_ct(request, ct):
    sahara.cluster_template_delete(request, ct['id'])


def delete_cluster(request, cluster):
    sahara.cluster_delete(request, cluster['id'])


def is_file_exist(path):
    if not u.is_path_exist(path):
        return False
    for name in file_name:
        if not u.is_path_exist(u.join_path(path, name)):
            return False
    return True


def recovery_instance(request, path, instances):
    for instance in instances:
        file_path = u.join_path(path, instance['instance_id'] + '.qcow2')
        image = glance.image_create(request, name=instance['instance_name'] + '-' + uuidutils.generate_uuid()[0:7],
                                    container_format='bare',
                                    disk_format='qcow2')
        glance.glanceclient(request).images.upload(image.id, open(file_path, 'rb'))
        instance['old_image_id'] = instance['id']
        instance['image_id'] = image.id
    return instances


def load_backup_file(path):
    cluster = u.read_from_json(u.join_path(path, "cluster.json"))
    ct = u.read_from_json(u.join_path(path, "clusterTemplate.json"))
    ngs = u.read_from_json(u.join_path(path, "node_groups.json"))
    flavors = u.read_from_json(u.join_path(path, "flavors.json"))
    key_pair = u.read_from_json(u.join_path(path, "keypair.json"))
    sgs = u.read_from_json(u.join_path(path, "security_groups.json"))
    images = u.read_from_json(u.join_path(path, "images.json"))
    return cluster, ct, ngs['node_groups'], flavors['flavors'], key_pair, sgs['security_groups'], images['images']


'''
ins = [
        {
            "old_image_id": "4134fec3-2058-4ede-9ad0-1f098599e0a2",
            "instance_id": "eaaed342-8a43-4633-8bc1-96745627fd34",
            "instance_name": "sandbox-sandbox-slave-0",
            "node_group_id": "4e01427f-a5ec-4295-a74c-7b87cac4758f",
            "image_id": "cf3282bb-4685-4cd3-8a43-3c00ee19a148"
        },
        {
            "old_image_id": "4134fec3-2058-4ede-9ad0-1f098599e0a2",
            "instance_id": "aad8c9d7-bdf2-46b3-a8e4-4af8c6ae8d40",
            "instance_name": "sandbox-sandbox-slave-1",
            "node_group_id": "4e01427f-a5ec-4295-a74c-7b87cac4758f",
            "image_id": "1c3d6124-ddaa-476e-8205-6b2c54d03e92"
        },
        {
            "old_image_id": "4134fec3-2058-4ede-9ad0-1f098599e0a2",
            "instance_id": "020e0978-7ddc-4c54-a4e3-3a15b6e3d2b6",
            "instance_name": "sandbox-sandbox-slave-2",
            "node_group_id": "4e01427f-a5ec-4295-a74c-7b87cac4758f",
            "image_id": "9096d6ed-46ef-4b8f-9139-0a668bdbddb9"
        },
        {
            "old_image_id": "4134fec3-2058-4ede-9ad0-1f098599e0a2",
            "instance_id": "009f8ae8-f4a4-4dad-a76f-9622a5739ec2",
            "instance_name": "sandbox-sandbox-master-0",
            "node_group_id": "fad35140-7317-4f26-b600-19788f9ff535",
            "image_id": "0cda4f53-2930-45d3-915e-c536f3af7864"
        }
    ]
'''


def recovery_ct(request, ct, node_groups):
    ct_ngs = get_ct_ng(node_groups)
    ct = sahara.cluster_template_create(request,
                                        name=ct['name'] + uuidutils.generate_uuid()[0:7],
                                        plugin_name=ct['plugin_name'],
                                        hadoop_version=ct['hadoop_version'],
                                        description=ct['description'],
                                        cluster_configs=ct['cluster_configs'],
                                        node_groups=ct_ngs,
                                        anti_affinity=ct['anti_affinity'],
                                        net_id=None,
                                        use_autoconfig=ct['use_autoconfig'],
                                        shares=ct['shares'],
                                        is_public=ct['is_public'],
                                        is_protected=ct['is_protected'],
                                        domain_name=ct['domain_name']
                                        ).to_dict()
    ct = sahara.cluster_template_get(request, ct['id']).to_dict()
    return ct


def get_ct_ng(node_groups):
    ct_ngs = []
    for ng in node_groups:
        ct_ng = {}
        ct_ng['node_group_template_id'] = ng['id']
        ct_ng['name'] = ng['name']
        ct_ng['count'] = 1
        ct_ngs.append(ct_ng)
    return ct_ngs


def recovery_ngs(request, node_groups, instances, flavor_id=None, security_groups=None, flavors=None, sgs=None):
    new_ngs = []
    for instance in instances:
        new_ng = recovery_ng(request, node_groups, instance, flavor_id, security_groups, flavors, sgs)
        new_ngs.append(new_ng)
    return new_ngs


def recovery_ng(request, node_groups, instance, flavor_id=None, security_groups=None, flavors=None, sgs=None):
    ng = get_ng(node_groups, instance)
    if flavor_id is None:
        flavor_id = get_flavor_id(ng, flavors)
    if security_groups is None:
        security_groups = get_sgs_id(ng, sgs)
    new_ng = sahara.nodegroup_template_create(request,
                                              name=instance['instance_name'] + uuidutils.generate_uuid()[0:7],
                                              plugin_name=ng['plugin_name'],
                                              hadoop_version=ng['hadoop_version'],
                                              flavor_id=flavor_id,
                                              description=ng['description'],
                                              volumes_per_node=ng['volumes_per_node'],
                                              volumes_size=ng['volumes_size'],
                                              node_processes=ng['node_processes'],
                                              node_configs=ng['node_configs'],
                                              floating_ip_pool=ng['floating_ip_pool'],
                                              security_groups=security_groups,
                                              auto_security_group=ng['auto_security_group'],
                                              availability_zone=ng['availability_zone'],
                                              volumes_availability_zone=ng['volumes_availability_zone'],
                                              volume_type=ng['volume_type'],
                                              image_id=instance['image_id'],
                                              is_proxy_gateway=ng['is_proxy_gateway'],
                                              volume_local_to_instance=ng['volume_local_to_instance'],
                                              use_autoconfig=ng['use_autoconfig'],
                                              shares=ng['shares'],
                                              is_public=ng['is_public'],
                                              is_protected=ng['is_protected']
                                              ).to_dict()
    new_ng = sahara.nodegroup_template_get(request, new_ng['id']).to_dict()
    new_ng['old_id'] = ng['id']
    return new_ng


def get_ng(node_groups, instance):
    for ng in node_groups:
        if ng['id'] == instance['node_group_id']:
            return ng
    return None


def get_flavor_id(ng, flavors):
    for flavor in flavors:
        if flavor['old_id'] == ng['flavor_id']:
            return flavor['id']
    return None


def get_sgs_id(ng, sgs):
    sgs_id = []
    for sg_id in ng['security_groups']:
        for sg in sgs:
            if sg['old_id'] == sg_id:
                sgs_id.append(sg['id'])
                break
    return sgs_id


def recovery_images(request, images, instances):
    for instance in instances:
        image = get_tags(instance['old_image_id'], images)
        sahara.image_update(request, instance['image_id'], image['username'], image['description'])
        sahara.image_tags_update(request, instance['image_id'], image['tags'])
    return True


def get_tags(instance_id, images):
    for image in images:
        if instance_id == image['id']:
            return image
    return None


def get_instances_ngs(cluster):
    ngs = cluster['node_groups']
    instances_list = []
    for ng in ngs:
        instances = ng['instances']
        for instance in instances:
            instance_info = {}
            instance_info['instance_id'] = instance['instance_id']
            instance_info['instance_name'] = instance['instance_name']
            instance_info['image_id'] = ng['image_id']
            instance_info['node_group_id'] = ng['node_group_template_id']
            instances_list.append(instance_info)
    return instances_list


def recovery_flavors(request, flavors):
    new_flavors = []
    for flavor in flavors:
        new_flavor = recovery_flavor(request, flavor)
        new_flavors.append(new_flavor)
    return new_flavors


def recovery_flavor(request, flavor):
    new_flavor = nova.flavor_create(request,
                                    name=flavor['name'] + uuidutils.generate_uuid()[0:7],
                                    memory=flavor['ram'],
                                    vcpu=flavor['vcpus'],
                                    disk=flavor['disk'],
                                    ephemeral=flavor['OS-FLV-EXT-DATA:ephemeral'],
                                    swap=get_flavor_swap(flavor['swap']),
                                    rxtx_factor=flavor['rxtx_factor']).to_dict()
    new_flavor = nova.flavor_get(request, new_flavor['id']).to_dict()
    new_flavor['old_id'] = flavor['id']
    return new_flavor


def get_flavor_swap(swap):
    if swap == "":
        return 0
    else:
        return int(swap)


def recovery_keypair(request, keypair):
    new_keypair = nova.keypair_import(request,
                                      keypair['name'] + uuidutils.generate_uuid()[0:7],
                                      keypair['public_key']).to_dict()
    new_keypair = nova.keypair_get(request, new_keypair['name']).to_dict()
    new_keypair['old_name'] = keypair['name']
    return new_keypair


def recovery_sgs(request, sgs):
    new_sgs = []
    for sg in sgs:
        new_sg = recovery_sg(request, sg)
        new_sgs.append(new_sg)
    return new_sgs


# TODO backup group_id
def recovery_sg(request, sg):
    sg_manager = nova.SecurityGroupManager(request)
    new_sg = sg_manager.create(sg['name'] + uuidutils.generate_uuid()[0:7], sg['description']).to_dict()
    sg_rules = sg['rules']
    for rule in sg_rules:
        sg_manager.rule_create(parent_group_id=new_sg['id'],
                               ip_protocol=rule['ip_protocol'],
                               from_port=rule['from_port'],
                               to_port=rule['to_port'],
                               cidr=rule['ip_range']['cidr'])
    new_sg = sg_manager.get(new_sg['id']).to_dict()
    new_sg['old_id'] = sg['id']
    return new_sg





























