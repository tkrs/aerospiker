package aerospiker.task

import cats._
import scalaz.concurrent.Task

package object syntax extends TaskInstances

abstract class TaskInstances extends TaskInstance1

sealed trait TaskInstance1 {
  implicit val TaskFlatMap: FlatMap[Task] = new FlatMap[Task] {
    override def flatMap[A, B](fa: Task[A])(f: (A) => Task[B]): Task[B] = fa flatMap f
    override def map[A, B](fa: Task[A])(f: (A) => B): Task[B] = fa map f
    override def ap[A, B](fa: Task[A])(f: Task[A => B]): Task[B] = fa.flatMap(a => f.map(ff => ff(a)))
  }
}
