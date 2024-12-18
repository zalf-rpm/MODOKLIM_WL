# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg-mohnicke@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

def get_fbp_attr(ip, attr_name):
    if ip.attributes and attr_name:
        for kv in ip.attributes:
            if kv.key == attr_name:
                return kv.value
    return None


def copy_and_set_fbp_attrs(old_ip, new_ip, **kwargs):
    # no attributes to be copied?
    if not old_ip.attributes and len(kwargs) == 0:
        return

    # is there an old attribute to be updated?
    attr_name_to_new_index = {}
    if old_ip.attributes and len(kwargs) > 0:
        for i, kv in enumerate(old_ip.attributes):
            if kv.key in kwargs:
                attr_name_to_new_index[kv.key] = i
                break

    # init space for attributes in new IP
    new_attrs_size = len(old_ip.attributes) if old_ip.attributes else 0
    for k, _ in kwargs.items():
        if k not in attr_name_to_new_index:
            new_attrs_size += 1
            attr_name_to_new_index[k] = new_attrs_size - 1
    attrs = new_ip.init("attributes", new_attrs_size)

    # copy old attributes
    if old_ip.attributes:
        indices = list(attr_name_to_new_index.values())
        for i, kv in enumerate(old_ip.attributes):
            if i not in indices:
                attrs[i].key = kv.key
                attrs[i].value = kv.value

    # set new attribute if there
    for attr_name, new_index in attr_name_to_new_index.items():
        attrs[new_index].key = attr_name
        attrs[new_index].value = kwargs[attr_name]


def update_config(config, argv, print_config=False, allow_new_keys=False):
    if len(argv) > 1:
        for arg in argv[1:]:
            kv = arg.split("=", maxsplit=1)
            if len(kv) < 2:
                continue
            k, v = kv
            if len(k) > 1 and k[:2] == "--":
                k = k[2:]
            if allow_new_keys or k in config:
                config[k] = v.lower() == "true" if v.lower() in ["true", "false"] else v
        if print_config:
            print(config)
