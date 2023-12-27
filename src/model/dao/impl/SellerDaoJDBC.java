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
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDao{

	//Preparando uma dependência para todos os métodos
	private Connection conex;
	public SellerDaoJDBC(Connection conex) {
		this.conex = conex;
	}
	
	@Override
	public void insert(Seller obj) {
		
		PreparedStatement st = null;
		
		try {
			st = conex.prepareStatement(
					"INSERT INTO seller "
					+"(Name, Email, BirthDate, BaseSalary, DepartmentId) "
					+"VALUES "
					+"(?, ?, ?, ?, ?)",
					Statement.RETURN_GENERATED_KEYS);// Comando para retorna o id do objeto inserido
			
			st.setString(1, obj.getName());
			st.setString(2, obj.getEmail());
			st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			st.setDouble(4, obj.getBaseSalary());
			st.setInt(5, obj.getDepartment().getId());
			
			int rowsAffected = st.executeUpdate();
			
			if(rowsAffected > 0) {
				ResultSet rs = st.getGeneratedKeys();//obter a chave gerado pelo bd para esse novo seller
				if(rs.next()) {
					int id = rs.getInt(1);//pegando o ID gerado pelo sql
					obj.setId(id);//incluindo o id gerado no objeto seller conforme esta no BD
				}
				DB.closeResultSet(rs);//Fechando dentro do if pois o result set não existe no escopo global da consulta
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
	public void update(Seller obj) {
		PreparedStatement st = null;
		
		try {
			st = conex.prepareStatement(
					"UPDATE seller "
					+"SET Name = ?, Email = ?, BirthDate = ?, BaseSalary = ?, DepartmentId = ? "
					+"WHERE Id = ?");
					
			st.setString(1, obj.getName());
			st.setString(2, obj.getEmail());
			st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			st.setDouble(4, obj.getBaseSalary());
			st.setInt(5, obj.getDepartment().getId());
			st.setInt(6, obj.getId());
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
			st = conex.prepareStatement("DELETE FROM seller WHERE Id = ?");
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
	public Seller findById(Integer id) {
		
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			st = conex.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+ " FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "WHERE seller.Id = ?");
					
			st.setInt(1, id);
			
			rs = st.executeQuery();
			
			if(rs.next()) {
				Department dp = instantiateDepartmen(rs); //as 2 instanciações estão como metodos para reutilização
				Seller obj = instantiateSeller(rs, dp);
				return obj;
			}
			return null;
		}
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st); // não precisa fechar a conexão pois ela pode ser reutilizada nos outros metodos
			DB.closeResultSet(rs); // então fecha a conexão no programa principal
		}
	}

	@Override
	public List<Seller> findByDepartment(Department department) {
		
		PreparedStatement st = null;
		ResultSet rs = null;
		
		List<Seller> lista = new ArrayList<>();		
		Map<Integer, Department> mapDepartment = new HashMap<>(); //Lista de Hashmap para evitar repetição na instanciação
		
		try {
			st = conex.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+"FROM seller INNER JOIN department "
					+"ON seller.DepartmentId = department.Id "
					+"WHERE DepartmentId = ? "
					+"ORDER BY Name");
			
			st.setInt(1, department.getId());
			rs = st.executeQuery();
			
			while(rs.next()) {
				Department dp = mapDepartment.get(rs.getInt("DepartmentId"));
				
				if (dp == null){ // se não existir o dp na lista ele vai criar uma nova instanciar 
					dp = instantiateDepartmen(rs);// pega os dados do resultset e instancia um novo departamento
					mapDepartment.put(rs.getInt("DepartmentId"), dp);//Ao fim adiciona o departamento nova no mapa
				}
				
				Seller obj = instantiateSeller(rs, dp);
				lista.add(obj);
			}
			return lista;
		}
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st); // não precisa fechar a conexão pois ela pode ser reutilizada nos outros metodos
			DB.closeResultSet(rs); // então fecha a conexão no programa principal
		}
	}
	
	@Override
	public List<Seller> findAll() {
		
		PreparedStatement st = null;
		ResultSet rs = null;
		
		List<Seller> lista = new ArrayList<>();		
		Map<Integer, Department> mapDepartment = new HashMap<>(); //Lista de Hashmap para evitar repetição na instanciação
		
		try {
			st = conex.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+"FROM seller INNER JOIN department "
					+"ON seller.DepartmentId = department.Id "
					+"ORDER BY Name");

			rs = st.executeQuery();
			
			while(rs.next()) {
				Department dp = mapDepartment.get(rs.getInt("DepartmentId"));
				
				if (dp == null){ // se não existir o dp na lista ele vai criar uma nova instanciar 
					dp = instantiateDepartmen(rs);// pega os dados do resultset e instancia um novo departamento
					mapDepartment.put(rs.getInt("DepartmentId"), dp);//Ao fim adiciona o departamento nova no mapa
				}
				
				Seller obj = instantiateSeller(rs, dp);
				lista.add(obj);
			}
			return lista;
		}
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st); // não precisa fechar a conexão pois ela pode ser reutilizada nos outros metodos
			DB.closeResultSet(rs); // então fecha a conexão no programa principal
		}
	}
	
	private Seller instantiateSeller(ResultSet rs, Department dp) throws SQLException {
		Seller obj = new Seller();
		obj.setId(rs.getInt("Id"));
		obj.setName(rs.getString("Name"));
		obj.setEmail(rs.getString("Email"));
		obj.setBirthDate(rs.getDate("BirthDate"));
		obj.setBaseSalary(rs.getDouble("BaseSalary"));
		obj.setDepartment(dp);
		return obj;
	}

	private Department instantiateDepartmen(ResultSet rs) throws SQLException {
		Department dp = new Department();
		dp.setId(rs.getInt("DepartmentId"));
		dp.setName(rs.getString("DepName"));
		return dp;
	}
}
