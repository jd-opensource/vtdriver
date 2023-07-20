/*
Copyright 2021 JD Project Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.jd.jdbc.tindexes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;

@Getter
public class TableDestinationGroup implements Comparable<TableDestinationGroup> {

    private final List<ActualTable> tables;

    public TableDestinationGroup() {
        this.tables = new ArrayList<>();
    }

    public TableDestinationGroup(List<ActualTable> tables) {
        if (tables == null) {
            this.tables = new ArrayList<>();
        } else {
            this.tables = tables;
        }
    }

    public void addActualTable(ActualTable actualTable) {
        this.tables.add(actualTable);
    }

    /**
     * return switch table map
     *
     * @return
     */
    public Map<String, String> getSwitchTableMap() {
        Map<String, String> switchTable = new HashMap<>(tables.size());
        for (ActualTable act : tables) {
            switchTable.put(act.getLogicTable().getFirstActualTableName(), act.getActualTableName());
        }
        return switchTable;
    }

    /**
     * return actualtable -> logictable map
     *
     * @return
     */
    public Map<String, String> getActLtbMap() {
        Map<String, String> actLtbMap = new HashMap<>();
        for (ActualTable act : tables) {
            actLtbMap.put(act.getActualTableName(), act.getLogicTable().getLogicTable());
        }
        return actLtbMap;
    }

    @Override
    public int compareTo(TableDestinationGroup o) {
        if (o == null) {
            return 1;
        }
        if (this == o) {
            return 0;
        }
        if (tables.size() > o.getTables().size()) {
            return 1;
        }
        if (tables.size() < o.getTables().size()) {
            return -1;
        }
        for (int i = 0; i < tables.size(); i++) {
            int result = tables.get(i).compareTo(o.getTables().get(i));
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableDestinationGroup)) {
            return false;
        }
        TableDestinationGroup anotherGroup = (TableDestinationGroup) o;
        if (tables.size() != anotherGroup.getTables().size()) {
            return false;
        }
        for (int i = 0; i < tables.size(); i++) {
            if (!tables.get(i).equals(anotherGroup.getTables().get(i))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tables);
    }
}
