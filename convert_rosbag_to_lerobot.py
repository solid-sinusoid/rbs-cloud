"""
  Convert_rosbag_demo_to_lerobot_dataset
  Script: convert rosbag demo episodes to LeRobot dataset
  Dependencies:
    lerobot ver 0.1.0 (https://github.com/huggingface/lerobot) commit 724874e063ecfb892bbcbc4a5e16fde5860cb28c (2025-07-15)
    rosbags ver 0.10.9

  Robossembler Team: @shalenikol release 0.1 2025-07-15
  Robossembler Team: @shalenikol release 0.2 2025-08-01 for rbs-cloud
"""
import sys
import json
import argparse
from typing import List
from pathlib import Path
from PIL import Image
import numpy as np
import cv2
import time

from cv_bridge import CvBridge
from rosbags.highlevel import AnyReader
from rosbags.typesys import Stores, get_typestore

from lerobot.datasets.lerobot_dataset import LeRobotDataset
from concurrent.futures import ThreadPoolExecutor
import multiprocessing

FPS = 30 # default
REPO_ID = "rbs_ros2bag"
SYNCED = "_synced.json"
USE_VIDEOS = False  # используем PNG-фреймы, так что формат — изображения

start_time = time.time()  # Запоминаем время начала

def convert_seconds(total_seconds) -> str:
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = int(total_seconds % 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"

def has_db3_file(directory) -> bool:
    return any(file.suffix == '.db3' for file in Path(directory).iterdir() if file.is_file())

def find_folders_with_db3_files(directory: Path) -> List[Path]:
    # Получаем все вложенные папки
    subfolders = [folder for folder in directory.iterdir() if folder.is_dir() and has_db3_file(folder)]
    # Сортируем список папок
    return sorted(subfolders)

def to_lerobot_dataset(json_file: Path, output_root: str, num_workers: int = 1):
    # === ЗАГРУЗКА ДАННЫХ ===
    with open(json_file, "r") as f:
        data = json.load(f)

    camera_keys = data["cameras"]
    image_shape = data["image_shape"]

    cam_features = [cam.lstrip('/').replace('/', '.') for cam in camera_keys]  # ['/robot_camera/depth_image', '/robot_camera/image']

    robot_joint_names = data["episodes"][0]["frames"][0]["joint_state"]["name"]
    nof_joints = len(robot_joint_names)
    estimated_fps = data["estimated_fps"]
    fps = FPS if estimated_fps < 0.1 else estimated_fps

    # === ОПРЕДЕЛЕНИЕ FEATURES ===
    features = {
        cam: {
            "dtype": "image",
            "shape": image_shape,
            "names": ["height", "width", "channels"]
        }
        for cam in cam_features
    }
    features.update({
        "observation.state": {
            "dtype": "float32",
            "shape": (nof_joints,),
            "names": robot_joint_names,
        },
    })
    features.update({
        "action": {
            "dtype": "float32",
            "shape": (nof_joints,),
            "names": robot_joint_names,
        },
    })

    # === СОЗДАНИЕ LeRobotDataset ===
    dataset = LeRobotDataset.create(
        repo_id=REPO_ID,
        fps=fps,
        root=output_root,
        features=features,
        use_videos=USE_VIDEOS,
    )

    def load_frame_data(frame):
        frame_data = {
            "observation.state": np.array(frame["joint_state"]["pos"], dtype=np.float32),
            "action": np.array(frame["joint_state"]["pos"], dtype=np.float32),
        }
        for cam_idx, cam in enumerate(camera_keys):
            image_path = Path(frame[cam])
            image = Image.open(image_path).convert("RGB")
            frame_data[cam_features[cam_idx]] = np.array(image)
        return frame_data

    for episode in data["episodes"]:
        frames = episode["frames"]
        if num_workers > 1:
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                frame_datas = list(executor.map(load_frame_data, frames))
        else:
            frame_datas = [load_frame_data(f) for f in frames]

        for frame_data in frame_datas:
            dataset.add_frame(frame_data, "default_task")

        # === СОХРАНЕНИЕ ЭПИЗОДА ===
        dataset.save_episode()

def add_episode(dir: Path) -> None:
    image_shape = None
    episode = {
        "bag": str(dir.resolve()),
        "eidx": add_episode.counter,
        "num_joint_state": 0,
        "joint_states": [],
        "num_frame": 0,
        "frames": []
    }
    e_image_dir = "episode" + str(add_episode.counter+1) # folder for images of the episode
    with AnyReader([dir], default_typestore=extract_rosbag_to_json.typestore) as reader:
        camera_topics = [] # for update
        frame_index = -1
        start_topic = ""
        num_js = 0
        for conn, timestamp, rawdata in reader.messages():
            topic = conn.topic

            if topic == extract_rosbag_to_json.joint_topic:
                msg = reader.deserialize(rawdata, conn.msgtype)
                def clean_array(arr):
                    return [0.0 if isinstance(v, float) and (v != v) else v for v in arr]  # NaN != NaN

                num_js += 1
                episode["joint_states"].append({
                    "timestamp": timestamp,
                    "joint_state": {
                        "name": list(msg.name),
                        "pos": clean_array(list(msg.position)),
                        "vel": clean_array(list(msg.velocity)),
                        "eff": clean_array(list(msg.effort)),
                    }
                })

            elif topic in extract_rosbag_to_json.camera_topics:
                msg = reader.deserialize(rawdata, conn.msgtype)
                try:
                    if topic in extract_rosbag_to_json.cim_topic:
                        img_cv = extract_rosbag_to_json.bridge.compressed_imgmsg_to_cv2(msg)
                    else:
                        img_cv = extract_rosbag_to_json.bridge.imgmsg_to_cv2(msg) #, desired_encoding="passthrough")

                    if img_cv is None:
                        continue
                    elif len(img_cv.shape) == 2:
                        img_cv = cv2.cvtColor(img_cv, cv2.COLOR_GRAY2RGB)
                    elif img_cv.shape[2] == 4:
                        img_cv = cv2.cvtColor(img_cv, cv2.COLOR_BGRA2RGB)
                    else:
                        img_cv = cv2.cvtColor(img_cv, cv2.COLOR_BGR2RGB)

                    if frame_index < 0:
                        start_topic = topic
                    if topic == start_topic:
                        if image_shape == None:
                            image_shape = img_cv.shape
                        frame_index += 1

                    camera_name = topic.lstrip('/').replace('/', '_')
                    img_filename = f"{e_image_dir}/{camera_name}/frame_{frame_index:06d}.png"
                    img_path = extract_rosbag_to_json.image_dir / img_filename
                    img_path.parent.parent.mkdir(exist_ok=True)
                    img_path.parent.mkdir(exist_ok=True)
                    Image.fromarray(img_cv).save(img_path)

                    episode["frames"].append({
                        "timestamp": timestamp,
                        topic: str(img_path) #.relative_to(Path.cwd())) # str(img_filename)
                    })
                    if topic not in camera_topics:
                        camera_topics.append(topic)

                except Exception as e:
                    print(f"[!] Failed to process image from {topic} @ {timestamp}: {img_cv=} {e}")

    if len(episode["frames"]):
        # only those topics will remain for which there are frames
        print(f"{camera_topics=}")
        if len(camera_topics) < len(extract_rosbag_to_json.camera_topics):
            extract_rosbag_to_json.camera_topics = camera_topics
            extract_rosbag_to_json.output["cameras"] = camera_topics

    episode["num_frame"] = frame_index + 1
    episode["num_joint_state"] = num_js
    extract_rosbag_to_json.output["episodes"].append(episode)
    extract_rosbag_to_json.image_shape = image_shape # applies to the entire dataset

    add_episode.counter += 1

def common_part_json(dir: Path) -> None:
    # common part
    with AnyReader([dir], default_typestore=extract_rosbag_to_json.typestore) as reader:
        # === Определяем топики ===
        for conn in reader.connections:
            if conn.msgtype == "sensor_msgs/msg/Image":
                extract_rosbag_to_json.im_topic.append(conn.topic)
            elif conn.msgtype == "sensor_msgs/msg/CompressedImage":
                extract_rosbag_to_json.cim_topic.append(conn.topic)

        # all camera topics
        camera_topics = extract_rosbag_to_json.im_topic + extract_rosbag_to_json.cim_topic
        # find only camera topics with data
        active_topics = {}
        for conn, _, _ in reader.messages():
            if conn.topic in camera_topics:
                active_topics[conn.topic] = None
        camera_topics = [t for t in active_topics.keys()]

        extract_rosbag_to_json.joint_topic = next(
            (conn.topic for conn in reader.connections if conn.msgtype == "sensor_msgs/msg/JointState"), None
        )
        if not extract_rosbag_to_json.joint_topic:
            raise ValueError("JointState topic not found in bag")

        # === Структура JSON ===
        extract_rosbag_to_json.output = {
            "cameras": camera_topics,
            "image_shape": [],
            "robots": [extract_rosbag_to_json.joint_topic],
            "num_episodes": 0,
            "episodes": []
        }
        extract_rosbag_to_json.camera_topics = camera_topics

def extract_rosbag_to_json(bag_path:Path, output_json:Path, synced_json_path:Path, output_image_dir:Path) -> None:
    print("Starting the export procedure..")

    extract_rosbag_to_json.image_dir = output_image_dir
    extract_rosbag_to_json.image_dir.mkdir(exist_ok=True, parents=True)

    extract_rosbag_to_json.bridge = CvBridge()

    extract_rosbag_to_json.typestore = get_typestore(Stores.ROS2_JAZZY)

    extract_rosbag_to_json.output = {}
    extract_rosbag_to_json.im_topic = []
    extract_rosbag_to_json.cim_topic = []
    extract_rosbag_to_json.camera_topics = []
    extract_rosbag_to_json.joint_topic = []
    extract_rosbag_to_json.image_shape = None

    list_bags = find_folders_with_db3_files(bag_path)
    common_part_json(list_bags[0])
    print(f"JSON common part: {extract_rosbag_to_json.output}")
    print(f"Total episodes: {len(list_bags)}")

    add_episode.counter = 0

    for bag in list_bags:
        add_episode(bag)
        print(f"episode {add_episode.counter}: ok")

    extract_rosbag_to_json.output["image_shape"] = extract_rosbag_to_json.image_shape
    extract_rosbag_to_json.output["num_episodes"] = len(list_bags) #add_episode.counter

    with open(output_json, "w") as f:
        json.dump(extract_rosbag_to_json.output, f, indent=2)

    print(f"✅ Export complete!\nJSON saved to: {output_json.resolve()}\nImages saved to: {extract_rosbag_to_json.image_dir.resolve()}")

    # === СОХРАНЕНИЕ СИНХРОНИЗИРОВАННОГО ВАРИАНТА ===
    fps = 0.0
    synced_output = {
        "bags": str(bag_path.resolve()),
        "estimated_fps": fps,
        "cameras": extract_rosbag_to_json.camera_topics,
        "image_shape": extract_rosbag_to_json.image_shape,
        "robots": [extract_rosbag_to_json.joint_topic],
        "num_episodes": len(list_bags),
        "episodes": []
    }
    rgb_topic = extract_rosbag_to_json.camera_topics[0]

    for episode in extract_rosbag_to_json.output["episodes"]:
        e_sync = {
            "bag": episode["bag"],
            "eidx": episode["eidx"],
            "num_frames": 0,
            "frames": []
        }

        rgb_msgs = [m for m in episode["frames"] if rgb_topic in m]
        joint_msgs = [m for m in episode["joint_states"]]

        num_frames = 0
        for im in rgb_msgs:
            # Находим ближайшее joint_state сообщение по timestamp
            closest = min(joint_msgs, key=lambda j: abs(j["timestamp"] - im["timestamp"]))

            # Добавляем все изображения (включая rgb_topic и другие камеры)
            cam_images = {}
            for cam_topic in extract_rosbag_to_json.camera_topics:
                # ищем изображение от этой камеры с таким же timestamp
                candidates = [m for m in episode["frames"] if cam_topic in m]
                match = min(candidates, key=lambda m: abs(m["timestamp"] - im["timestamp"])) if candidates else None
                if match:
                    cam_key = cam_topic
                    cam_images[cam_key] = match[cam_topic]

            e_sync["frames"].append({
                "timestamp": im["timestamp"],
                "idx": num_frames,
                "joint_state": closest["joint_state"],
                **cam_images  # добавляем все камеры
            })
            num_frames += 1

        e_sync["num_frames"] = num_frames

        if num_frames >= 2 and fps <= 0.0:
            t0 = e_sync["frames"][0]["timestamp"]
            t1 = e_sync["frames"][-1]["timestamp"]
            duration_sec = (t1 - t0) / 1e9  # наносекунды → секунды
            fps = (num_frames - 1) / duration_sec if duration_sec > 0 else 0.0
            synced_output["estimated_fps"] = fps
            print(f"📈 Estimated FPS (synced): {fps:.1f}")

        synced_output["episodes"].append(e_sync)

    # synced_json_path = output_json.with_name(output_json.stem + SYNCED)
    with open(synced_json_path, "w") as f:
        json.dump(synced_output, f, indent=2)

    print(f"🧩 Synced JSON saved to: {synced_json_path.resolve()}")
    return #synced_json_path

def main():
    parser = argparse.ArgumentParser(description="Convert ROS2 bag to JSON + image folder (LeRobot format)")
    parser.add_argument("bag", help="Path to folder with ROS2 bag episode files")
    parser.add_argument("--output", default="./converted_dataset", help="Directory to store dataset")
    parser.add_argument("--json", default="ros2bag_msg.json", help="Path to output JSON file")
    parser.add_argument("--images", default="frames", help="Directory to store extracted images")
    parser.add_argument("--workers", type=int, default=multiprocessing.cpu_count(), help="Number of worker threads for image loading")
    args = parser.parse_args()

    bag = Path(args.bag)
    if not bag.is_dir():
        print(f"[!] {args.bag} is not a folder")
        sys.exit()

    dataset_dir = Path(args.output)
    x = 0
    while dataset_dir.is_dir():
        dataset_dir = Path(args.output + str(x))
        x += 1
    if x:
        Path(args.output).rename(dataset_dir)
        # print(f"[✔] Конвертация пропущена: данные уже существуют в '{dataset_dir}'")

    out_json = Path(args.json)
    synced = out_json.with_name(out_json.stem + SYNCED)
    frames = Path(args.images)

    extract_rosbag_to_json(bag, out_json, synced, frames)

    to_lerobot_dataset(synced, args.output, num_workers=args.workers)

    end_time = time.time()  # время окончания
    execution_time = end_time - start_time
    print(f"*****************\nВремя выполнения: {convert_seconds(execution_time)}\n")

if __name__ == "__main__":
    main()
