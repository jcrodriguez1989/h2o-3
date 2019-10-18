package hex.grid;

import hex.Model;
import hex.ModelBuilder;
import hex.ParallelModelBuilder;
import hex.genmodel.utils.DistributionFamily;
import hex.tree.gbm.GBMModel;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.Benchmark;
import water.*;
import water.fvec.Frame;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static hex.genmodel.utils.DistributionFamily.AUTO;
import static org.junit.Assert.*;

public class GridTest extends TestUtil {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    TestUtil.stall_till_cloudsize(1);
  }

  @Test
  public void testParallelModelBuild() {
    final List<Model> models = new ArrayList<>();
    Scope.enter();
    try {
      // Create new GBM model
      final Frame trainingFrame = parse_test_file("./smalldata/testng/airlines_train.csv");
      Scope.track(trainingFrame);
      GBMModel.GBMParameters parms = new GBMModel.GBMParameters();
      parms._train = trainingFrame._key;
      parms._distribution = AUTO;
      parms._response_column = "IsDepDelayed";
      parms._ntrees = 10;
      final Lock lock = new ReentrantLock();

      // Must be thread safe, otherwise won't work and will lock itself at the end.
      final BiConsumer<Model, ParallelModelBuilder> modelFeder = (model, parallelModelBuilder) -> {
        lock.lock();
        try {
          models.add(model);
          if (models.size() < 3) {
            parallelModelBuilder.modelInProgressCounter.decrementAndGet();
            parallelModelBuilder.run(new ModelBuilder[]{ModelBuilder.make(parms.clone())});
          } else {
            parallelModelBuilder.modelInProgressCounter.decrementAndGet();
            parallelModelBuilder.noMoreModels();
          }
        } finally {
          lock.unlock();
        }
      };
      
      final ModelBuilder modelBuilder = ModelBuilder.make(parms.clone());
      final ParallelModelBuilder parallelModelBuilder = new ParallelModelBuilder(modelFeder);
      parallelModelBuilder.run(new ModelBuilder[]{modelBuilder});
      parallelModelBuilder.join();
      models.forEach(Scope::track_generic);
      assertEquals(3, models.size());
    } finally {
      Scope.exit();
    }

  }

  @Test
  public void testParallelGridSearch() {
    try {
      Scope.enter();

      final Frame trainingFrame = parse_test_file("./smalldata/testng/airlines_train.csv");
      Scope.track(trainingFrame);

      final Integer[] ntreesArr = new Integer[]{50, 51, 52, 53, 54, 55};
      final Integer[] maxDepthArr = new Integer[]{2, 3, 4, 5, 6, 7};
      HashMap<String, Object[]> hyperParms = new HashMap<String, Object[]>() {{
        put("_distribution", new DistributionFamily[]{DistributionFamily.multinomial});
        put("_ntrees", ntreesArr);
        put("_max_depth", maxDepthArr);
      }};

      GBMModel.GBMParameters params = new GBMModel.GBMParameters();
      params._train = trainingFrame._key;
      params._response_column = "IsDepDelayed";

      final long time = System.currentTimeMillis();
      for (int i = 0; i < 10; i++) {
        Job<Grid> gs = GridSearch.startGridSearch(null, params, hyperParms);
        Scope.track_generic(gs);
        gs.get();
      }

      System.out.println(String.format("Took %d millis on average", (System.currentTimeMillis() - time) / 10));
    } finally {
      Scope.exit();
    }
  }

  @Test
  public void testFaileH2OdParamsCleanup() throws FileNotFoundException {
    try {
      Scope.enter();
      final Frame trainingFrame = parse_test_file("smalldata/iris/iris_train.csv");
      Scope.track(trainingFrame);

      // Setup random hyperparameter search space
      HashMap<String, Object[]> hyperParms = new HashMap<String, Object[]>() {{
        put("_distribution", new DistributionFamily[]{DistributionFamily.multinomial});
        put("_ntrees", new Integer[]{5});
        put("_max_depth", new Integer[]{2});
        put("_min_rows", new Integer[]{5000000}); // Invalid hyperparameter, causes model training to fail
        put("_learn_rate", new Double[]{.7});
      }};

      GBMModel.GBMParameters params = new GBMModel.GBMParameters();
      params._train = trainingFrame._key;
      params._response_column = "species";

      Job<Grid> gs = GridSearch.startGridSearch(null, params, hyperParms);
      Scope.track_generic(gs);
      final Grid errGrid = gs.get();
      Scope.track_generic(errGrid);

      assertEquals(0, errGrid.getModelCount());

      final Grid.SearchFailure failures = errGrid.getFailures();
      assertEquals(1, failures.getFailureCount());
      assertEquals(1, failures.getFailedParameters().length);
      assertEquals(1, failures.getFailedRawParameters().length);
      assertEquals(1, failures.getFailureDetails().length);
      assertEquals(1, failures.getFailureStackTraces().length);

      // Check if the error is related to the specified invalid hyperparameter
      final String expectedErr = "Details: ERRR on field: _min_rows: The dataset size is too small to split for min_rows=5000000.0: must have at least 1.0E7 (weighted) rows";
      assertTrue(failures.getFailureStackTraces()[0].contains(expectedErr));

      //Set the parameter to an acceptable value
      hyperParms.put("_min_rows", new Integer[]{10});
      gs = GridSearch.startGridSearch(errGrid._key, params, hyperParms); // It is important to target the previously created grid
      Scope.track_generic(gs);
      final Grid grid = gs.get();
      Scope.track_generic(grid);

      // There should be no errors, one resulting model in the grid with previously supplied parameters
      assertEquals(1, grid.getModelCount());
      assertTrue(grid.getModels()[0] instanceof GBMModel);
      assertEquals(10, ((GBMModel) grid.getModels()[0])._parms._min_rows, 0);

      final Grid.SearchFailure secondRunFailures = grid.getFailures();
      assertEquals(0, secondRunFailures.getFailureCount());
      assertEquals(0, secondRunFailures.getFailedParameters().length);
      assertEquals(0, secondRunFailures.getFailedRawParameters().length);
      assertEquals(0, secondRunFailures.getFailureDetails().length);
      assertEquals(0, secondRunFailures.getFailureStackTraces().length);

    } finally {
      Scope.exit();
    }
  }

  @Test
  public void gridSearchExportCheckpointsDir() throws IOException {
    try {
      Scope.enter();

      final Frame trainingFrame = parse_test_file("smalldata/iris/iris_train.csv");
      Scope.track(trainingFrame);

      HashMap<String, Object[]> hyperParms = new HashMap<String, Object[]>() {{
        put("_distribution", new DistributionFamily[]{DistributionFamily.multinomial});
        put("_ntrees", new Integer[]{5});
        put("_max_depth", new Integer[]{2});
        put("_learn_rate", new Double[]{.7});
      }};

      GBMModel.GBMParameters params = new GBMModel.GBMParameters();
      params._train = trainingFrame._key;
      params._response_column = "species";
      params._export_checkpoints_dir = temporaryFolder.newFolder().getAbsolutePath();

      Job<Grid> gs = GridSearch.startGridSearch(null, params, hyperParms);
      Scope.track_generic(gs);
      final Grid originalGrid = gs.get();
      Scope.track_generic(originalGrid);

      final File serializedGridFile = new File(params._export_checkpoints_dir, originalGrid._key.toString());
      assertTrue(serializedGridFile.exists());
      assertTrue(serializedGridFile.isFile());
      
      final Grid grid = loadGridFromFile(serializedGridFile);
      assertArrayEquals(originalGrid.getModelKeys(), grid.getModelKeys());
      Scope.track_generic(grid);
    } finally {
      Scope.exit();
    }
  }

  @Test
  public void gridSearchManualExport() throws IOException {
    try {
      Scope.enter();

      final Frame trainingFrame = parse_test_file("smalldata/iris/iris_train.csv");
      Scope.track(trainingFrame);

      HashMap<String, Object[]> hyperParms = new HashMap<String, Object[]>() {{
        put("_distribution", new DistributionFamily[]{DistributionFamily.multinomial});
        put("_ntrees", new Integer[]{5});
        put("_max_depth", new Integer[]{2});
        put("_learn_rate", new Double[]{.7});
      }};

      GBMModel.GBMParameters params = new GBMModel.GBMParameters();
      params._train = trainingFrame._key;
      params._response_column = "species";

      final String exportDir = temporaryFolder.newFolder().getAbsolutePath();

      Job<Grid> gs = GridSearch.startGridSearch(null, params, hyperParms);
      Scope.track_generic(gs);
      final Grid originalGrid = gs.get();
      Scope.track_generic(originalGrid);
      
      final String originalGridPath = exportDir + "/" + originalGrid._key.toString();
      originalGrid.exportBinary(originalGridPath);
      assertTrue(Files.exists(Paths.get(originalGridPath)));
      
      originalGrid.exportModelsBinary(exportDir);
      
      for(Model model : originalGrid.getModels()){
        assertTrue(Files.exists(Paths.get(exportDir, model._key.toString())));  
      }
    } finally {
      Scope.exit();
    }
  }

  private static Grid loadGridFromFile(final File file) throws IOException {
    try (final FileInputStream fileInputStream = new FileInputStream(file)) {
      final AutoBuffer autoBuffer = new AutoBuffer(fileInputStream);
      final Freezable possibleGrid = autoBuffer.get();
      assertTrue(possibleGrid instanceof Grid);
      return (Grid) possibleGrid;
      
    }
  }


  /**
   * Failed parameters related to an existing model should be retained after repeated launch of grid search
   */
  @Test
  public void testFailedParamsRetention() {
    try {
      Scope.enter();
      final Frame trainingFrame = parse_test_file("smalldata/iris/iris_train.csv");
      Scope.track(trainingFrame);

      // Setup random hyperparameter search space
      HashMap<String, Object[]> hyperParms = new HashMap<String, Object[]>() {{
        put("_distribution", new DistributionFamily[]{DistributionFamily.multinomial});
        put("_ntrees", new Integer[]{5});
        put("_max_depth", new Integer[]{2});
        put("_min_rows", new Integer[]{10}); // Invalid hyperparameter, causes model training to fail
        put("_learn_rate", new Double[]{.7});
      }};

      GBMModel.GBMParameters params = new GBMModel.GBMParameters();
      params._train = trainingFrame._key;
      params._response_column = "species";

      Job<Grid> gs = GridSearch.startGridSearch(null, params, hyperParms);
      Scope.track_generic(gs);
      final Grid errGrid = gs.get();
      Scope.track_generic(errGrid);

      assertEquals(1, errGrid.getModelCount());

      Grid.SearchFailure failures = errGrid.getFailures();
      assertEquals(0, failures.getFailureCount());

      errGrid.appendFailedModelParameters(errGrid.getModels()[0]._key, params, new RuntimeException("Test exception"));
      
      failures = errGrid.getFailures();
      assertEquals(1, failures.getFailureCount());

      // Train a second model with modified parameters to forcefully produce a new model
      hyperParms.put("_learn_rate", new Double[]{0.5});
      gs = GridSearch.startGridSearch(errGrid._key, params, hyperParms);
      Scope.track_generic(gs);
      final Grid grid = gs.get();
      Scope.track_generic(grid);

      // There should be no errors, one resulting model in the grid with previously supplied parameters
      assertEquals(2, grid.getModelCount());
      assertTrue(grid.getModels()[0] instanceof GBMModel);
      assertTrue(grid.getModels()[1] instanceof GBMModel);

      final Grid.SearchFailure secondRunFailures = grid.getFailures();
      assertEquals(1, secondRunFailures.getFailureCount());

      final String expectedErr = "Test exception";
      assertTrue(failures.getFailureStackTraces()[0].contains(expectedErr));

    } finally {
      Scope.exit();
    }
  }

}
