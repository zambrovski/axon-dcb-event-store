package io.event.thinking.sample.faculty.commandhandler;

import io.event.thinking.eventstore.api.Index;

import static io.event.thinking.eventstore.api.Index.index;

public class FacultyIndices {

    public static final String STUDENT_ID = "studentId";
    public static final String COURSE_ID = "courseId";

    private FacultyIndices() {

    }

    public static Index courseIdIndex(String courseId) {
        return index(COURSE_ID, courseId);
    }

    public static Index studentIdIndex(String studentId) {
        return index(STUDENT_ID, studentId);
    }
}
