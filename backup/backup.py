
from sahara_dashboard.api import sahara as sahara
import utils as u
from openstack_dashboard.api import nova
from openstack_dashboard.api import glance
import time
import io
import os


def backup_info(request, cluster_id, path=None):
    if path is None:
        path = "/home/backup/"
    path = u.join_path(path, cluster_id)
    try:
        u.create_dir(path)
        ct_id, ukp_id, default_image_id, instance_ids = save_cluster_info(request, cluster_id, path)
        node_groups_template_ids = save_cluster_template_info(request, ct_id, path)
        flavor_ids, image_ids, security_group_ids = save_node_groups_info(request, node_groups_template_ids, path)
        image_ids = append_to_list(default_image_id, image_ids)
        save_key_pair(request, ukp_id, path)
        save_flavors_info(request, flavor_ids, path)
        save_security_group_info(request, security_group_ids, path)
        save_images_tag(request, image_ids, path)
        #backup_instance(request, instance_ids, path)
    except Exception:
        u.delete_dir(path)
        return False
    else:
        return True


def backup_instance(request, instance_id_list, image_path):

    for instance_id in instance_id_list:
        # Create a snapshot of an instance
        snapshot_name = 'snapshot_of_' + instance_id
        image_id = nova.snapshot_create(request, instance_id, snapshot_name)

        # Waiting for creating snapshot
        image = glance.image_get(request, image_id)
        while image.status != 'active':
            time.sleep(5)
            image = glance.image_get(request, image_id)

        # Download image data
        image_data = glance.glanceclient(request).images.data(image.id)

        image_filename = u.join_path(image_path, instance_id+'.raw')
        f = io.open(image_filename, 'wb')
        for chunk in image_data:
            f.write(chunk)

        glance.image_delete(request, image.id)

        # TODO: Transform image format from raw to qcow2
        os.system('qemu-img convert -f raw -O qcow2 ' + image_filename + ' ' + image_filename + '.qcow2')


def save_cluster_info(request, cluster_id, path):
    cluster_info = sahara.cluster_get(request, cluster_id).to_dict()
    path = u.join_path(path, "cluster.json")
    u.save_to_json(path, cluster_info)
    ct_id = cluster_info['cluster_template_id']
    ukp_id = cluster_info['user_keypair_id']
    default_image_id = cluster_info['default_image_id']
    instance_ids = get_instances_id(cluster_info['node_groups'])
    return ct_id, ukp_id, default_image_id, instance_ids


def get_instances_id(node_groups):
    instances = []
    for ng in node_groups:
        _instances = ng['instances']
        for instance in _instances:
            instances.append(instance['instance_id'])
    return instances


def save_cluster_template_info(request, ct_id, path):
    ct_info = sahara.cluster_template_get(request, ct_id).to_dict()
    path = u.join_path(path, "clusterTemplate.json")
    node_groups_template_ids = []
    _node_groups = ct_info['node_groups']
    for ng in _node_groups:
        node_groups_template_ids.append(ng['node_group_template_id'])
    u.save_to_json(path, ct_info)
    return node_groups_template_ids


def save_node_groups_info(request, node_groups_template_ids, path):
    path = u.join_path(path, "node_groups.json")
    node_groups = []
    flavor_ids = []
    image_ids = []
    security_group_ids = []
    for ng_id in node_groups_template_ids:
        ng_info = sahara.nodegroup_template_get(request, ng_id).to_dict()
        node_groups.append(ng_info)
        flavor_ids = append_to_list(ng_info['flavor_id'], flavor_ids)
        image_ids = append_to_list(ng_info['image_id'], image_ids)
        for sgi in ng_info['security_groups']:
            security_group_ids = append_to_list(sgi, security_group_ids)
    ngs_info = {"node_groups": node_groups}
    u.save_to_json(path, ngs_info)
    return flavor_ids, image_ids, security_group_ids


def append_to_list(element, _list):
    if element not in _list:
        _list.append(element)
    return _list


def save_flavors_info(request, flavors_ids, path):
    path = u.join_path(path, "flavors.json")
    flavors = []
    for f_id in flavors_ids:
        flavor_info = nova.flavor_get(request, f_id).to_dict()
        flavors.append(flavor_info)
    flavors_info = {"flavors": flavors}
    u.save_to_json(path, flavors_info)


def save_key_pair(request, user_key_pair_id, path):
    key_pair_info = nova.keypair_get(request, user_key_pair_id).to_dict()
    path = u.join_path(path, "keypair.json")
    u.save_to_json(path, key_pair_info)
    return True


def save_security_group_info(request, security_group_ids, path):
    path = u.join_path(path, "security_groups.json")
    sg = []
    sg_manager = nova.SecurityGroupManager(request)
    for sg_id in security_group_ids:
        sg_info = sg_manager.get(sg_id).to_dict()
        sg.append(sg_info)
    sgs_info = {"security_groups": sg}
    u.save_to_json(path, sgs_info)


def save_images_tag(request, image_ids, path):
    path = u.join_path(path, "images.json")
    images = []
    for image_id in image_ids:
        image_info = sahara.image_get(request, image_id).to_dict()
        images.append(image_info)
    images_info = {"images": images}
    u.save_to_json(path, images_info)
