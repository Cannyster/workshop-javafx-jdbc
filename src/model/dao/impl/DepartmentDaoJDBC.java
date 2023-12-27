package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import model.dao.DepartmentDao;
import model.entities.Department;

public class DepartmentDaoJDBC implements DepartmentDao{
	private Connection conex;
	public DepartmentDaoJDBC(Connection conex) {
		this.conex = conex;
	}
	
	@Override
	public void insert(Department dp) {
		PreparedStatement st = null;
		
		try {
			st = conex.prepareStatement("INSERT INTO department (Name) VALUES (?)",Statement.RETURN_GENERATED_KEYS);
			st.setString(1, dp.getName());
			int rowsAffected = st.executeUpdate();
			
			if(rowsAffected > 0) {
				ResultSet rs = st.getGeneratedKeys();
				if(rs.next()) {
					int id = rs.getInt(1);
					dp.setId(id);
				}
				DB.closeResultSet(rs);
			}
			else {
				throw new DbException("Erro Inesperado!! nenhum linha foi afetada");
			}
		}
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public void update(Department dp) {
		PreparedStatement st = null;
		
		try {
			st = conex.prepareStatement("UPDATE department SET Name = ? WHERE Id = ?");	
			st.setString(1, dp.getName());
			st.setInt(2, dp.getId());
			st.executeUpdate();	
		}
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public void deleteById(Integer id) {
		PreparedStatement st = null;
	
		try {
			st = conex.prepareStatement("DELETE FROM department WHERE Id = ?");
			st.setInt(1, id);
			st.executeUpdate();	
		}
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public Department findById(Integer id) {
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			st = conex.prepareStatement("SELECT * FROM department WHERE Id = ?");
			st.setInt(1, id);
			rs = st.executeQuery();
			
			if(rs.next()) {
				return instantiateDepartmen(rs);
			}
			else {
				return null;
			}
		}			
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs); 
		}
	}
	
	@Override
	public List<Department> findAll() {
		PreparedStatement st = null;
		ResultSet rs = null;
		List<Department> listaDP = new ArrayList<>();		
		Map<Integer, Department> mapDepartment = new HashMap<>();
		
		try {
			st = conex.prepareStatement("SELECT * FROM department ORDER BY Name");
			rs = st.executeQuery();
			
			while(rs.next()) {
				Department dp = mapDepartment.get(rs.getInt("Id"));
				
				if (dp == null){
					dp = instantiateDepartmen(rs);
					mapDepartment.put(rs.getInt("Id"), dp);
				}
				
				dp = instantiateDepartmen(rs);
				listaDP.add(dp);
			}
			return listaDP;
		}
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

	private Department instantiateDepartmen(ResultSet rs) throws SQLException {
		Department dp = new Department();
		dp.setId(rs.getInt("Id"));
		dp.setName(rs.getString("Name"));
		return dp;
	}
}
