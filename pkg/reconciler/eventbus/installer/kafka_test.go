package installer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/argoproj/argo-events/pkg/apis/events/v1alpha1"
	"github.com/argoproj/argo-events/pkg/shared/logging"
)

const (
	testKafkaName = "test-kafka"
	testKafkaURL  = "kafka:9092"
)

var (
	testKafkaExoticBus = &v1alpha1.EventBus{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.SchemeGroupVersion.String(),
			Kind:       "EventBus",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testKafkaName,
		},
		Spec: v1alpha1.EventBusSpec{
			Kafka: &v1alpha1.KafkaBus{
				URL: testKafkaURL,
			},
		},
	}
	testKafkaExoticBusWithURLSecret = &v1alpha1.EventBus{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.SchemeGroupVersion.String(),
			Kind:       "EventBus",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testKafkaName,
		},
		Spec: v1alpha1.EventBusSpec{
			Kafka: &v1alpha1.KafkaBus{
				URLSecret: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "kafka-secret"},
					Key:                   "url",
				},
			},
		},
	}
	testKafkaExoticBusBothURLs = &v1alpha1.EventBus{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.SchemeGroupVersion.String(),
			Kind:       "EventBus",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testKafkaName,
		},
		Spec: v1alpha1.EventBusSpec{
			Kafka: &v1alpha1.KafkaBus{
				URL: testKafkaURL,
				URLSecret: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "kafka-secret"},
					Key:                   "url",
				},
			},
		},
	}
	testKafkaExoticBusNoURL = &v1alpha1.EventBus{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.SchemeGroupVersion.String(),
			Kind:       "EventBus",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testKafkaName,
		},
		Spec: v1alpha1.EventBusSpec{
			Kafka: &v1alpha1.KafkaBus{},
		},
	}
)

func TestInstallationKafkaExotic(t *testing.T) {
	t.Run("installation with exotic kafka config", func(t *testing.T) {
		installer := NewExoticKafkaInstaller(testKafkaExoticBus, logging.NewArgoEventsLogger())
		conf, err := installer.Install(context.TODO())
		assert.NoError(t, err)
		assert.NotNil(t, conf.Kafka)
		assert.Equal(t, conf.Kafka.URL, testKafkaURL)
	})
}

func TestInstallationKafkaExoticWithURLSecret(t *testing.T) {
	t.Run("installation with kafka urlSecret config", func(t *testing.T) {
		installer := NewExoticKafkaInstaller(testKafkaExoticBusWithURLSecret, logging.NewArgoEventsLogger())
		conf, err := installer.Install(context.TODO())
		assert.NoError(t, err)
		assert.NotNil(t, conf.Kafka)
		assert.NotNil(t, conf.Kafka.URLSecret)
		assert.Equal(t, "kafka-secret", conf.Kafka.URLSecret.Name)
		assert.Equal(t, "url", conf.Kafka.URLSecret.Key)
	})
}

func TestInstallationKafkaExoticBothURLsFails(t *testing.T) {
	t.Run("installation fails when both url and urlSecret are specified", func(t *testing.T) {
		installer := NewExoticKafkaInstaller(testKafkaExoticBusBothURLs, logging.NewArgoEventsLogger())
		_, err := installer.Install(context.TODO())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "mutually exclusive")
	})
}

func TestInstallationKafkaExoticNoURLFails(t *testing.T) {
	t.Run("installation fails when neither url nor urlSecret is specified", func(t *testing.T) {
		installer := NewExoticKafkaInstaller(testKafkaExoticBusNoURL, logging.NewArgoEventsLogger())
		_, err := installer.Install(context.TODO())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "either url or urlSecret must be specified")
	})
}

func TestUninstallationKafkaExotic(t *testing.T) {
	t.Run("uninstallation with exotic kafka config", func(t *testing.T) {
		installer := NewExoticKafkaInstaller(testKafkaExoticBus, logging.NewArgoEventsLogger())
		err := installer.Uninstall(context.TODO())
		assert.NoError(t, err)
	})
}
