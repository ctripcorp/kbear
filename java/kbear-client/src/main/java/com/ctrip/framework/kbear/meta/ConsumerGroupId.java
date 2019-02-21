package com.ctrip.framework.kbear.meta;

public class ConsumerGroupId implements Cloneable {

    private String groupName;
    private String topicId;

    public ConsumerGroupId() {

    }

    public ConsumerGroupId(String groupName, String topicId) {
        this.groupName = groupName;
        this.topicId = topicId;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getTopicId() {
        return topicId;
    }

    public void setTopicId(String topicId) {
        this.topicId = topicId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((groupName == null) ? 0 : groupName.hashCode());
        result = prime * result + ((topicId == null) ? 0 : topicId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ConsumerGroupId other = (ConsumerGroupId) obj;
        if (groupName == null) {
            if (other.groupName != null)
                return false;
        } else if (!groupName.equals(other.groupName))
            return false;
        if (topicId == null) {
            if (other.topicId != null)
                return false;
        } else if (!topicId.equals(other.topicId))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "ConsumerGroupId [groupName=" + groupName + ", topicId=" + topicId + "]";
    }

    @Override
    public ConsumerGroupId clone() {
        try {
            return (ConsumerGroupId) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

}
