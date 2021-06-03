package recipes

import (
	"errors"

	"github.com/go-playground/validator/v10"
	"github.com/odpf/meteor/domain"
	"github.com/odpf/meteor/extractors"
	"github.com/odpf/meteor/processors"
	"github.com/odpf/meteor/sinks"
)

type Service struct {
	recipeStore    Store
	extractorStore *extractors.Store
	processorStore *processors.Store
	sinkStore      *sinks.Store
	validator      *validator.Validate
}

func NewService(recipeStore Store, extractorStore *extractors.Store, processorStore *processors.Store, sinkStore *sinks.Store) *Service {
	validator := validator.New()

	return &Service{
		recipeStore:    recipeStore,
		extractorStore: extractorStore,
		processorStore: processorStore,
		sinkStore:      sinkStore,
		validator:      validator,
	}
}

func (s *Service) Create(recipe domain.Recipe) error {
	err := s.validator.Struct(recipe)
	if err != nil {
		return InvalidRecipeError{err.Error()}
	}

	return s.recipeStore.Create(recipe)
}

func (s *Service) Run(recipe domain.Recipe) (*domain.Run, error) {
	run := s.buildRun(recipe)

	for i := 0; i < len(run.Tasks); i++ {
		data, err := s.runTask(&run.Tasks[i], run.Data)
		run.Data = data

		if err != nil {
			return run, err
		}
	}

	return run, nil
}

func (s *Service) Find(name string) (domain.Recipe, error) {
	return s.recipeStore.GetByName(name)
}

func (s *Service) runTask(task *domain.Task, data []map[string]interface{}) (result []map[string]interface{}, err error) {
	result = data

	switch task.Type {
	case domain.TaskTypeExtract:
		result, err = s.runExtractor(task.Name, task.Config)
	case domain.TaskTypeProcess:
		result, err = s.runProcessor(task.Name, data, task.Config)
	case domain.TaskTypeSink:
		err = s.runSink(task.Name, data, task.Config)
	default:
		err = errors.New("invalid task type")
	}

	if err != nil {
		err = newRunTaskError(*task, err)
		task.Status = domain.TaskStatusFailed
	} else {
		task.Status = domain.TaskStatusComplete
	}

	return result, err
}

func (s *Service) runExtractor(name string, config map[string]interface{}) (result []map[string]interface{}, err error) {
	extractor, err := s.extractorStore.Find(name)
	if err != nil {
		return result, err
	}

	return extractor.Extract(config)
}

func (s *Service) runProcessor(name string, data []map[string]interface{}, config map[string]interface{}) (result []map[string]interface{}, err error) {
	processor, err := s.processorStore.Find(name)
	if err != nil {
		return result, err
	}

	return processor.Process(data, config)
}

func (s *Service) runSink(name string, data []map[string]interface{}, config map[string]interface{}) (err error) {
	sink, err := s.sinkStore.Find(name)
	if err != nil {
		return err
	}

	return sink.Sink(data, config)
}

func (s *Service) buildRun(recipe domain.Recipe) *domain.Run {
	var tasks []domain.Task

	tasks = append(tasks, domain.Task{
		Type:   domain.TaskTypeExtract,
		Status: domain.TaskStatusReady,
		Name:   recipe.Source.Type,
		Config: recipe.Source.Config,
	})

	for _, processor := range recipe.Processors {
		tasks = append(tasks, domain.Task{
			Type:   domain.TaskTypeProcess,
			Status: domain.TaskStatusReady,
			Name:   processor.Name,
			Config: processor.Config,
		})
	}

	for _, sink := range recipe.Sinks {
		tasks = append(tasks, domain.Task{
			Type:   domain.TaskTypeSink,
			Status: domain.TaskStatusReady,
			Name:   sink.Name,
			Config: sink.Config,
		})
	}

	return &domain.Run{
		Recipe: recipe,
		Tasks:  tasks,
	}
}
