package hex;

import jsr166y.ForkJoinTask;
import water.util.IcedAtomicInt;
import water.util.Log;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Dispatcher for parallel model building. Starts building models every time the run method is invoked.
 * After each model is finished building, the `modelFinished` method is invoked, which in turn invokes modelFeeder callback.
 * ModelFeeder receives the model built and can deal with it in any way - e.g. put it into a Grid, or discard it if the resulting model
 * is a failure. It also has the power to invoke the training of any number of new models. Or stop the parallel model builder,
 * released the barrier insed.
 * 
 * During model training, there are no waiting loops and slept threads - everything is invoked just in time based on a callback. The only
 * waiting loop is present at the end, when there are no more models to be trained, yet some are still in the process of training.
 * 
 * This class is a POC and represents an idea, rather then being a final piece of code.
 * 
 */
public class ParallelModelBuilder extends ForkJoinTask<ParallelModelBuilder> {

  private BiConsumer<Model, ParallelModelBuilder> _modelFeeder;
  public IcedAtomicInt modelInProgressCounter = new IcedAtomicInt();

  public ParallelModelBuilder(BiConsumer<Model, ParallelModelBuilder> modelFeeder) {
    Objects.requireNonNull(modelFeeder);
    _modelFeeder = modelFeeder;
  }

  public void run(ModelBuilder[] modelBuilders) {
      for (final ModelBuilder modelBuilder : modelBuilders) {
        modelInProgressCounter.incrementAndGet();
        final Consumer<Model> consumer = this::modelFinished;
        modelBuilder.trainModel(consumer);
      }

  }

  private void modelFinished(final Model m) {
    _modelFeeder.accept(m, this);
  }

  public void noMoreModels() {
    int modelsInprogress;
    do {
      modelsInprogress = modelInProgressCounter.get();
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Log.err(e);
      }
    } while (modelsInprogress > 0);
    complete(this);
  }


  @Override
  public ParallelModelBuilder getRawResult() {
    return null;
  }

  @Override
  protected void setRawResult(ParallelModelBuilder value) {

  }

  @Override
  protected boolean exec() {
    return true;
  }
}
