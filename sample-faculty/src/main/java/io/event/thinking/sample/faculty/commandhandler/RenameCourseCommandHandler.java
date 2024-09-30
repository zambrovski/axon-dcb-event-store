package io.event.thinking.sample.faculty.commandhandler;

import io.event.thinking.eventstore.api.Criteria;
import io.event.thinking.eventstore.api.Criterion;
import io.event.thinking.micro.es.DcbCommandHandler;
import io.event.thinking.micro.es.Event;
import io.event.thinking.sample.faculty.api.command.RenameCourse;
import io.event.thinking.sample.faculty.api.event.CourseCreated;
import io.event.thinking.sample.faculty.api.event.CourseRenamed;

import java.util.List;
import java.util.function.Function;

import static io.event.thinking.eventstore.api.Criteria.anyOf;
import static io.event.thinking.eventstore.api.Criterion.allOf;
import static io.event.thinking.micro.es.Event.event;
import static io.event.thinking.micro.es.Indices.typeIndex;
import static io.event.thinking.sample.faculty.commandhandler.FacultyIndices.courseIdIndex;

public class RenameCourseCommandHandler implements DcbCommandHandler<RenameCourse, RenameCourseCommandHandler.State> {

  @Override
  public Criteria criteria(RenameCourse renameCourse) {
    // composed of state criteria
    return anyOf(
        NoCourseCreatedState.stateCriteria().apply(renameCourse),
        NamedCourseState.stateCriteria().apply(renameCourse)
    );
  }

  @Override
  public State initialState() {
    return State.initial();
  }

  @Override
  public State source(Object event, State state) {
    // could go to framework and do dynamic dispatch from there based on sealed state
    return switch (state) {
      case NoCourseCreatedState s -> s.evolve(event);
      case NamedCourseState s -> s.evolve(event);
      default -> throw new IllegalStateException("No handler for this event");
    };
  }

  @Override
  public List<Event> handle(RenameCourse command, State state) {
    // could go to framework and do dynamic dispatch from there based on sealed state
    return switch (state) {
      case NoCourseCreatedState s -> s.handle(command);
      case NamedCourseState s -> s.handle(command);
      default -> throw new IllegalStateException("Invalid state");
    };
  }

  public interface State {

    <T extends State> T evolve(Object event);

    List<Event> handle(RenameCourse command)

    static NoCourseCreatedState initial() {
      return new NoCourseCreatedState();
    }
  }

  /**
   * Initial state (no course created)
   */
  public record NoCourseCreatedState() implements State {

    public static Function<Object, Criterion> stateCriteria() {
      // this student subscribed to this course
      return (o) -> {
        RenameCourse renameCourse = (RenameCourse) o;
        return allOf(typeIndex(CourseCreated.NAME),
            courseIdIndex(
                renameCourse.courseId() // TAG deduction from the command
            )
        );
      };
    }

    @Override
    public State evolve(Object event) {
      if (event instanceof CourseCreated) {
        return new NamedCourseState(((CourseCreated) event).name());
      }
      throw new RuntimeException("Can't evolve using " + event);
    }

    public List<Event> handle(RenameCourse command) {
      throw new RuntimeException("Course does not exist");
    }

  }

  public record NamedCourseState(String name) implements State {

    public static Function<Object, Criterion> stateCriteria() {
      // this student subscribed to this course
      return (o) -> {
        RenameCourse renameCourse = (RenameCourse) o;
        return allOf(
            typeIndex(CourseRenamed.NAME),
            courseIdIndex(
                renameCourse.courseId()
            )
        );
      };
    }

    @Override
    public State evolve(Object event) {
      if (event instanceof CourseRenamed) {
        return new NamedCourseState(((CourseRenamed) event).newName());
      }
      throw new RuntimeException("Can't evolve using " + event);
    }

    public List<Event> handle(RenameCourse command) {
      if (this.name.equals(command.newName())) {
        throw new RuntimeException("Course already has the name " + name);
      }
      CourseRenamed payload = new CourseRenamed(command.courseId(), command.newName());
      return List.of(
          event(
              payload,
              typeIndex(CourseRenamed.NAME),
              courseIdIndex(command.courseId())
          )
      );
    }
  }
}
