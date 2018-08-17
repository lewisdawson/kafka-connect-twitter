/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.twitter;


import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

public class TwitterSourceConnectorTest {

  private TwitterSourceConnector connector;

  @BeforeEach
  void beforeEach() {

  }



  @Test
  public void test() {
    Set<String> filterKeywords = new HashSet<>();
    filterKeywords.add("123");
    filterKeywords.add("234");
    filterKeywords.add("345");
    filterKeywords.add("456");
    filterKeywords.add("567");
    filterKeywords.add("678");
    filterKeywords.add("789");
    filterKeywords.add("8910");


    // 3 lists, 7 el = [[1,2], [3,4], [5,6,7]]

    int maxTasks = 1;
    int numPerPartition = (int)Math.ceil((double)filterKeywords.size() / maxTasks);
    System.out.println("numPerPartition=" + numPerPartition);
    final int tasks = Math.min(maxTasks, filterKeywords.size());
    System.out.println("tasks=" + tasks);
    final List<Map<String, String>> taskConfigs = new ArrayList<>();

    Iterable<List<String>> partitions = Iterables.partition(filterKeywords, numPerPartition);
    for (List<String> k : partitions) {
      Map<String, String> taskSettings = new HashMap<>();

      if (!k.isEmpty()) {
        taskSettings.put(TwitterSourceConnectorConfig.FILTER_KEYWORDS_CONF, Joiner.on(',').join(k));
        taskConfigs.add(taskSettings);

        System.out.println(taskSettings.get(TwitterSourceConnectorConfig.FILTER_KEYWORDS_CONF));
      }
    }

    System.out.println("taskConfigsSize=" + taskConfigs.size());
  }

}
