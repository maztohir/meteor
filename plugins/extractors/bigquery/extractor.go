package bigquery

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/mitchellh/mapstructure"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"github.com/odpf/meteor/utils"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type Config struct {
	ProjectID          string `mapstructure:"project_id" validate:"required"`
	ServiceAccountJSON string `mapstructure:"service_account_json"`
}

type Extractor struct {
	logger plugins.Logger
	client *bigquery.Client
	ctx    context.Context
}

func New(logger plugins.Logger, client *bigquery.Client, ctx context.Context) extractor.TableExtractor {
	return &Extractor{
		logger: logger,
		client: client,
		ctx:    ctx,
	}
}

func (e *Extractor) Extract(configMap map[string]interface{}) (result []meta.Table, err error) {
	e.logger.Info("extracting kafka metadata...")
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return result, extractor.InvalidConfigError{}
	}
	err = e.validateConfig(config)
	if err != nil {
		return
	}

	if e.ctx != nil {
		e.ctx = context.Background()
	}

	if e.client != nil {
		e.client, err = e.createClient(config)
		if err != nil {
			return
		}
	}
	result, err = e.getMetadata()
	if err != nil {
		return
	}

	return
}

func (e *Extractor) getMetadata() (results []meta.Table, err error) {
	it := e.client.Datasets(e.ctx)

	dataset, err := it.Next()
	for err == nil {
		results, err = e.appendTablesMetadata(results, dataset)
		if err != nil {
			return
		}

		dataset, err = it.Next()

		break
	}
	if err == iterator.Done {
		err = nil
	}

	return
}

func (e *Extractor) appendTablesMetadata(results []meta.Table, dataset *bigquery.Dataset) ([]meta.Table, error) {
	it := dataset.Tables(e.ctx)

	table, err := it.Next()
	for err == nil {
		tableResult, err := e.mapTable(table)
		if err != nil {
			break
		} else {
			results = append(results, tableResult)
		}
		table, err = it.Next()

		break
	}
	if err == iterator.Done {
		err = nil
	}

	return results, err
}

func (e *Extractor) mapTable(t *bigquery.Table) (result meta.Table, err error) {
	tableMetadata, err := e.client.Dataset(t.DatasetID).Table(t.TableID).Metadata(e.ctx)
	result = meta.Table{
		Urn:         fmt.Sprintf("%s.%s.%s", t.ProjectID, t.DatasetID, t.TableID),
		Name:        t.TableID,
		Source:      "bigquery",
		Description: t.DatasetID,
		Schema:      e.extractSchema(tableMetadata.Schema),
	}
	return result, err
}

func (e *Extractor) extractSchema(t []*bigquery.FieldSchema) (columns *facets.Columns) {
	var columnList []*facets.Column
	for _, b := range t {
		columnList = append(columnList, e.mapColumn(b))
	}
	return &facets.Columns{
		Columns: columnList,
	}
}

func (e *Extractor) mapColumn(t *bigquery.FieldSchema) *facets.Column {
	columnProfile, _ := e.findColumnProfile(t)
	return &facets.Column{
		Name:        t.Name,
		Description: t.Description,
		DataType:    string(t.Type),
		IsNullable:  !(t.Required || t.Repeated),
		Profile:     columnProfile,
	}
}

func (e *Extractor) findColumnProfile(t *bigquery.FieldSchema) (*facets.ColumnProfile, error) {
	rows, err := e.profileTheColumn()
	if err != nil {
		return nil, err
	}
	result, err := e.getResult(rows)

	return &facets.ColumnProfile{
		Min:    result.Min,
		Max:    result.Max,
		Avg:    result.Avg,
		Med:    result.Med,
		Unique: result.Unique,
		Count:  result.Count,
		Top:    result.Top,
	}, err
}

func (e *Extractor) profileTheColumn() (*bigquery.RowIterator, error) {

	query := e.client.Query(
		`select 
		"1" AS min,
		"1" AS max,
		1.0 AS avg,
		1.0 AS med,
		1 AS unique,
		1 AS count,
		"1" AS top`)
	return query.Read(e.ctx)
}

type ResultRow struct {
	Min    string  `bigquery:"min"`
	Max    string  `bigquery:"max"`
	Avg    float32 `bigquery:"avg"`
	Med    float32 `bigquery:"med"`
	Unique int64   `bigquery:"unique"`
	Count  int64   `bigquery:"count"`
	Top    string  `bigquery:"top"`
}

func (e *Extractor) getResult(iter *bigquery.RowIterator) (ResultRow, error) {
	var row ResultRow
	err := iter.Next(&row)
	if err == iterator.Done {
		return row, nil
	}
	if err != nil {
		return row, fmt.Errorf("error iterating through results: %v", err)
	}

	return row, err
}

func (e *Extractor) createClient(config Config) (*bigquery.Client, error) {
	if config.ServiceAccountJSON == "" {
		e.logger.Info("credentials are not specified, creating bigquery client using Default Credentials...")
		return bigquery.NewClient(e.ctx, config.ProjectID)
	}

	return bigquery.NewClient(e.ctx, config.ProjectID, option.WithCredentialsJSON([]byte(config.ServiceAccountJSON)))
}

func (e *Extractor) getConfig(configMap map[string]interface{}) (config Config, err error) {
	err = mapstructure.Decode(configMap, &config)
	if err != nil {
		return
	}

	return
}

func (e *Extractor) validateConfig(config Config) (err error) {
	if config.ProjectID == "" {
		return errors.New("project_id is required")
	}

	return
}
