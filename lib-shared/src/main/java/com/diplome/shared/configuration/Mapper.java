package com.diplome.shared.configuration;

import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class Mapper {
    private final ModelMapper modelMapper;

    public Mapper(ModelMapper modelMapper) {
        this.modelMapper = modelMapper;
    }

    public <D, E> D map(E entity, Class<D> dClass) {
        return modelMapper.map(entity, dClass);
    }

    public <D, E> void map(E entity, D dEntity) {
        modelMapper.map(entity, dEntity);
    }

    public <D, E> List<D> map(List<E> entities, Class<D> dClass) {
        return entities.stream().map(entity -> modelMapper.map(entity, dClass)).collect(Collectors.toList());
    }
}
