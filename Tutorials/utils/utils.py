# Copyright 2020 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Extra functions to use during the notebooks."""

import datetime
import json
import random
import time
from google.cloud import pubsub_v1

words_test = ['to', 'beseech', 'pray', 'sheets', 'he']

words_no_sw_test = ['judgment', 'fellow', 'friend']

sw_test = ['To', 'to']


def data_generator(notebook_number):
  """Generates data to publish."""
  names = ['Laia', 'Victor', 'Alvaro', 'Guillem', 'Berta', 'Inigo']

  if notebook_number == 6:
    name = random.choice(names)
    score = random.randint(0, 99)
    timestamp = str(datetime.datetime.now())

    return {'name': name, 'score': score, 'timestamp': timestamp}

  if notebook_number == 7:
    name = random.choice(names)
    spent = random.randint(0, 99)

    return {'name': name, 'spent': spent}

  else:
    return {}


def publish_to_topic(n, topic, project_id, notebook_number, time_division=1):
  """Publishes to PubSub using the notebook to send the right data."""
  def publish(data, publisher):
    if not isinstance(data, str):
      data = json.dumps(data)

    data = data.encode('utf-8')
    publisher.publish(topic_path, data=data)

  publisher = pubsub_v1.PublisherClient()
  topic_path = publisher.topic_path(project_id, topic)

  for _ in range(n):
    time.sleep(random.random() / time_division)
    publish(data_generator(notebook_number), publisher)
