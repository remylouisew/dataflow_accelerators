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

"""Solutions for exercises in notebooks."""

notebook1 = {
    'not_perfect_squares': [(3, 1.7320508075688772), (1729, 41.58124577258358)],
    'perfect_squares': [(1024, 32.0), (1, 1.0), (49, 7.0)]}

notebook2 = [1, 29, 17, 7, 1729, 3]

notebook3 = {
    'total_per_items': [('Controller', 1), ('HDMI', 1), ('TV', 2),
                        ('Speaker', 2)],
    'total_sum': [6],
    'buyers': [('Bob', ['HDMI', 'Speaker', 'TV']), ('Pedro', ['Speaker']),
               ('Alice', ['Controller', 'TV'])]
}

notebook4 = {
    'avg': [1500.0, 1500.0, 1750.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0],
    'total': [('Marina', 4000), ('Cristina', 4000), ('Cristina', 2000),
              ('Juan', 2000), ('Juan', 1000)]
}

notebook5 = [6045]

notebook7 = {
    'wordcount': [('to', 438), ('he', 137), ('beseech', 8), ('pray', 18),
                  ('sheets', 2)],
    'modified_wordcount': [('friend', 30), ('fellow', 36), ('judgment', 14)]
}

solutions = {
    1: notebook1,
    2: notebook2,
    3: notebook3,
    4: notebook4,
    5: notebook5,
    7: notebook7
}
