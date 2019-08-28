/*
 * Copyright 2019 Hopper
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

package zookeeper;

public enum TaskStatus {

    KEY_STATUS("status"),
    KEY_WORKERID("workerId"),


    PENDING("Pending"),
    ASIGNED("Asigned"),
    DONE("Done"),
    FAILED("Failed"),
    DELETED("deleted");

    private String text;

    TaskStatus(String text) {
        this.text = text;
    }

    public static TaskStatus fromString(String text) {
        for (TaskStatus b : TaskStatus.values()) {
            if (b.text.equalsIgnoreCase(text)) {
                return b;
            }
        }
        return null;
    }
    public String getText() {
        return this.text;
    }
}
