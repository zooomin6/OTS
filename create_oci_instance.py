#!/usr/bin/env python3
"""OCI A1 Flex 인스턴스 자동 생성 — capacity 날 때까지 재시도.

두 가지 모드:
  - 로컬:  ~/.oci/config 사용, capacity 날 때까지 무한 재시도 (PC 켜둬야 함)
            python create_oci_instance.py
  - CI:    환경변수로 인증, --once 로 1회(=AD 한 바퀴) 시도 후 종료 (cron이 반복)
            python create_oci_instance.py --once
            → .github/workflows/oci-retry.yml 에서 15분마다 호출

중복 생성 방지: 이미 A1 인스턴스가 있으면 만들지 않음 (무료티어 한도 초과 방지).
모든 secret/식별자는 환경변수에서 로드 (.env 또는 GitHub Secrets).
"""
import os
import sys
import json
import time
import argparse
import urllib.request

import oci
from dotenv import load_dotenv

load_dotenv()

# ── 설정 (전부 env에서) ──
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
COMPARTMENT_ID     = os.environ.get("OCI_COMPARTMENT_ID", "")
SUBNET_ID          = os.environ.get("OCI_SUBNET_ID", "")
INSTANCE_NAME      = os.environ.get("OCI_INSTANCE_NAME", "ots-server")
SHAPE              = "VM.Standard.A1.Flex"
OCPUS              = int(os.environ.get("OCI_OCPUS", "4"))
MEMORY_GB          = int(os.environ.get("OCI_MEMORY_GB", "24"))
BOOT_VOLUME_GB     = int(os.environ.get("OCI_BOOT_GB", "100"))
RETRY_INTERVAL     = int(os.environ.get("OCI_RETRY_INTERVAL", "10"))  # 재시도 간격(초)

# 로컬 모드 fallback 경로 (CI에선 env로 대체됨)
OCI_CONFIG_FILE = os.path.expanduser(os.environ.get("OCI_CONFIG_FILE", r"C:\Users\sotus\.oci\config"))
SSH_KEY_FILE    = os.path.expanduser(os.environ.get("SSH_KEY_FILE", r"C:\Users\sotus\.ssh\ots_oracle.pub"))


def build_config():
    """env에 OCI_PRIVATE_KEY 있으면 CI 모드(dict 인증), 없으면 로컬 config 파일."""
    if os.environ.get("OCI_PRIVATE_KEY"):
        config = {
            "user":        os.environ["OCI_USER_OCID"],
            "fingerprint": os.environ["OCI_FINGERPRINT"],
            "tenancy":     os.environ["OCI_TENANCY_OCID"],
            "region":      os.environ["OCI_REGION"],
            "key_content": os.environ["OCI_PRIVATE_KEY"],
        }
        oci.config.validate_config(config)
        return config
    return oci.config.from_file(OCI_CONFIG_FILE)


def get_ssh_public_key():
    if os.environ.get("SSH_PUBLIC_KEY"):
        return os.environ["SSH_PUBLIC_KEY"].strip()
    with open(SSH_KEY_FILE) as f:
        return f.read().strip()


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": text}).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"  (텔레그램 알림 실패: {e})")


def existing_a1_instance(compute_client):
    """이미 살아있는(=한도 차지하는) A1 인스턴스가 있으면 반환 → 중복 생성 방지."""
    alive = {"PROVISIONING", "RUNNING", "STARTING", "STOPPING", "STOPPED"}
    instances = compute_client.list_instances(compartment_id=COMPARTMENT_ID).data
    for ins in instances:
        if ins.shape == SHAPE and ins.lifecycle_state in alive:
            return ins
    return None


def get_ubuntu_arm_image_id(compute_client):
    images = compute_client.list_images(
        compartment_id=COMPARTMENT_ID,
        operating_system="Canonical Ubuntu",
        operating_system_version="22.04",
        shape=SHAPE, sort_by="TIMECREATED", sort_order="DESC",
    ).data
    if not images:
        raise RuntimeError("Ubuntu 22.04 ARM (aarch64) image not found in this region")
    return images[0].id


def get_ads(identity_client):
    ads = identity_client.list_availability_domains(compartment_id=COMPARTMENT_ID).data
    return [ad.name for ad in ads]


def try_create(compute_client, ad, image_id, ssh_key):
    details = oci.core.models.LaunchInstanceDetails(
        compartment_id=COMPARTMENT_ID, display_name=INSTANCE_NAME,
        availability_domain=ad, shape=SHAPE,
        shape_config=oci.core.models.LaunchInstanceShapeConfigDetails(
            ocpus=OCPUS, memory_in_gbs=MEMORY_GB),
        source_details=oci.core.models.InstanceSourceViaImageDetails(
            source_type="image", image_id=image_id, boot_volume_size_in_gbs=BOOT_VOLUME_GB),
        create_vnic_details=oci.core.models.CreateVnicDetails(
            subnet_id=SUBNET_ID, assign_public_ip=True),
        metadata={"ssh_authorized_keys": ssh_key},
    )
    return compute_client.launch_instance(details).data


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true",
                    help="AD 한 바퀴만 시도 후 종료. 미지정 시 무한 재시도(로컬).")
    ap.add_argument("--max-seconds", type=int, default=0,
                    help="이 시간(초)만 재시도 후 종료 (CI/cron용). 0=무제한.")
    args = ap.parse_args()

    config = build_config()
    compute_client = oci.core.ComputeClient(config)
    identity_client = oci.identity.IdentityClient(config)
    ssh_key = get_ssh_public_key()

    # 중복 생성 방지 가드
    existing = existing_a1_instance(compute_client)
    if existing:
        print(f"이미 A1 인스턴스 존재: {existing.display_name} ({existing.lifecycle_state}) — 생성 건너뜀.")
        sys.exit(0)

    ads = get_ads(identity_client)
    image_id = get_ubuntu_arm_image_id(compute_client)
    print(f"ADs={ads} / image={image_id}")

    start_t = time.time()
    i = 0
    while True:
        ad = ads[i % len(ads)]
        i += 1
        print(f"[{i}] AD={ad} ...", end=" ", flush=True)
        try:
            instance = try_create(compute_client, ad, image_id, ssh_key)
            print("SUCCESS!")
            send_telegram(
                f"✅ OCI 인스턴스 생성 성공!\n"
                f"Name: {instance.display_name}\nID: {instance.id}\n"
                f"2분 후 OCI 콘솔 > Compute > Instances 에서 Public IP 확인.\n\n"
                f"⚠️ 이제 GitHub Actions의 oci-retry 워크플로우를 비활성화하세요."
            )
            sys.exit(0)
        except oci.exceptions.ServiceError as e:
            msg = getattr(e, "message", str(e))
            if "Out of host capacity" in msg or e.status in (429, 500, 503):
                print("out of capacity")
                if args.once and i >= len(ads):
                    sys.exit(0)
                if args.max_seconds and (time.time() - start_t) >= args.max_seconds:
                    print(f"max-seconds({args.max_seconds}s) 도달 — 종료 (cron이 재시도)")
                    sys.exit(0)
                if not args.once:
                    time.sleep(RETRY_INTERVAL)
            else:
                print(f"FATAL (HTTP {e.status}): {msg}")
                sys.exit(1)
        except Exception as e:
            # 네트워크 일시 끊김(connection aborted 등) → 치명적 아님. 재시도 (워크플로우 안 죽임)
            print(f"일시 오류, 재시도: {e}")
            if args.once and i >= len(ads):
                sys.exit(0)
            if args.max_seconds and (time.time() - start_t) >= args.max_seconds:
                sys.exit(0)
            if not args.once:
                time.sleep(RETRY_INTERVAL)


if __name__ == "__main__":
    main()
