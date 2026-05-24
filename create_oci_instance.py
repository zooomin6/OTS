#!/usr/bin/env python3
"""OCI A1 Flex instance auto-creation script - retries until capacity available."""

import oci
import time
import sys

OCI_CONFIG_FILE  = r"C:\Users\sotus\.oci\config"
COMPARTMENT_ID   = "ocid1.tenancy.oc1..aaaaaaaaotmasgjymixa7bnezxjel5oocbjg473i3yh6yvvmj3fgbb5htvjq"
SUBNET_ID        = "ocid1.subnet.oc1.ap-chuncheon-1.aaaaaaaafbk7c6rdytg4q5yqlczpr4ug2gop6fzkcuwqmtczs5wy7pa22m4q"
SSH_KEY_FILE     = r"C:\Users\sotus\.ssh\ots_oracle.pub"
INSTANCE_NAME    = "ots-server"
SHAPE            = "VM.Standard.A1.Flex"
OCPUS            = 4
MEMORY_GB        = 24
BOOT_VOLUME_GB   = 100
RETRY_INTERVAL   = 10  # seconds between retries


def get_availability_domains(identity_client):
    ads = identity_client.list_availability_domains(compartment_id=COMPARTMENT_ID).data
    return [ad.name for ad in ads]


def get_ubuntu_arm_image_id(compute_client):
    images = compute_client.list_images(
        compartment_id=COMPARTMENT_ID,
        operating_system="Canonical Ubuntu",
        operating_system_version="22.04",
        shape=SHAPE,
        sort_by="TIMECREATED",
        sort_order="DESC",
    ).data

    if not images:
        # fallback: filter by display name
        all_images = compute_client.list_images(
            compartment_id=COMPARTMENT_ID,
            operating_system="Canonical Ubuntu",
            operating_system_version="22.04",
            sort_by="TIMECREATED",
            sort_order="DESC",
        ).data
        images = [img for img in all_images if "aarch64" in img.display_name.lower()]

    if not images:
        raise RuntimeError("Ubuntu 22.04 ARM (aarch64) image not found in this region")

    print(f"Image: {images[0].display_name}  ({images[0].id})")
    return images[0].id


def try_create(compute_client, availability_domain, image_id, ssh_public_key):
    details = oci.core.models.LaunchInstanceDetails(
        compartment_id=COMPARTMENT_ID,
        display_name=INSTANCE_NAME,
        availability_domain=availability_domain,
        shape=SHAPE,
        shape_config=oci.core.models.LaunchInstanceShapeConfigDetails(
            ocpus=OCPUS,
            memory_in_gbs=MEMORY_GB,
        ),
        source_details=oci.core.models.InstanceSourceViaImageDetails(
            source_type="image",
            image_id=image_id,
            boot_volume_size_in_gbs=BOOT_VOLUME_GB,
        ),
        create_vnic_details=oci.core.models.CreateVnicDetails(
            subnet_id=SUBNET_ID,
            assign_public_ip=True,
        ),
        metadata={"ssh_authorized_keys": ssh_public_key},
    )
    return compute_client.launch_instance(details)


def main():
    print("=== OCI A1 Flex Instance Creator ===\n")

    config          = oci.config.from_file(OCI_CONFIG_FILE)
    compute_client  = oci.core.ComputeClient(config)
    identity_client = oci.identity.IdentityClient(config)

    with open(SSH_KEY_FILE) as f:
        ssh_public_key = f.read().strip()
    print(f"SSH key loaded.")

    print("Fetching availability domains...")
    ads = get_availability_domains(identity_client)
    print(f"ADs: {ads}\n")

    print("Fetching Ubuntu 22.04 ARM image...")
    image_id = get_ubuntu_arm_image_id(compute_client)
    print()

    attempt  = 0
    ad_index = 0

    while True:
        attempt += 1
        ad = ads[ad_index % len(ads)]
        print(f"[{attempt}] AD={ad} ...", end=" ", flush=True)

        try:
            resp     = try_create(compute_client, ad, image_id, ssh_public_key)
            instance = resp.data
            print("SUCCESS!")
            print(f"\n  Instance ID : {instance.id}")
            print(f"  Name        : {instance.display_name}")
            print(f"  State       : {instance.lifecycle_state}")
            print("\nPublic IP will appear in OCI Console > Compute > Instances in ~2 min.")
            break

        except oci.exceptions.ServiceError as e:
            msg = getattr(e, "message", str(e))
            if "Out of host capacity" in msg or e.status in (429, 500):
                print(f"out of capacity — retry in {RETRY_INTERVAL}s")
                ad_index += 1
                time.sleep(RETRY_INTERVAL)
            else:
                print(f"FATAL (HTTP {e.status}): {msg}")
                sys.exit(1)

        except Exception as e:
            print(f"FATAL: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
